package main

import (
	"encoding/json"
	"fmt"
	"github.com/tealeg/xlsx"
	"io"
	"net/http"
	"net/url"
	"sync"
)

type Coordinates struct {
	Latitude  float64
	Longitude float64
	Err       error
}

func GetLocation(fullAddress string) (Coordinates, error) {
	apiKey := "7a248fac386644af875d6abd3488d879"

	// Construct the URL with the query parameters
	baseURL := "https://api.opencagedata.com/geocode/v1/json"
	queryParams := url.Values{}
	queryParams.Set("q", fullAddress)
	queryParams.Set("key", apiKey)
	fullUrl := baseURL + "?" + queryParams.Encode()

	// Send the HTTP GET request
	resp, err := http.Get(fullUrl)
	if err != nil {
		fmt.Println("Error:", err)
		return Coordinates{}, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error:", err)
		}
	}(resp.Body)

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error:", err)
		return Coordinates{}, err
	}

	// Parse the JSON response
	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		fmt.Println("Error:", err)
		return Coordinates{}, err
	}

	// Extract the latitude and longitude values
	if results, ok := data["results"].([]interface{}); ok && len(results) > 0 {
		if geometry, ok := results[0].(map[string]interface{})["geometry"].(map[string]interface{}); ok {
			latitude := geometry["lat"].(float64)
			longitude := geometry["lng"].(float64)
			fmt.Printf("Full Address: %v\n", fullAddress)
			fmt.Printf("Lat: %f, Long: %f\n", latitude, longitude)
			return Coordinates{Latitude: latitude, Longitude: longitude, Err: nil}, nil
		} else {
			return Coordinates{}, fmt.Errorf("unable to extract coordinates")
		}
	} else {
		return Coordinates{}, fmt.Errorf("geocoding API request failed")
	}
}

func main() {
	//Open the XLSX file
	file, err := xlsx.OpenFile("SdxStores_Prod_20240304.xlsx")
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}

	// Get the first sheet
	sheet := file.Sheets[0]

	// Get the values in column "BH"
	column := "BH"
	values := getColumnValues(sheet, column)
	if len(values) == 0 {
		fmt.Printf("No values found in column %s.\n", column)
		return
	}

	// Create a wait group to wait for goroutines to finish
	var wg sync.WaitGroup

	// Create a buffered channel to control the number of concurrent goroutines
	concurrency := 10 // Number of goroutines to run in parallel
	semaphore := make(chan struct{}, concurrency)

	// Launch goroutine for even row indices
	wg.Add(1)
	go func() {
		defer wg.Done()
		for rowIndex, value := range values {
			// Skip the first row (titles) and odd row indices
			if rowIndex == 0 || rowIndex%2 == 1 {
				continue
			}

			// Acquire a semaphore to control the concurrency
			semaphore <- struct{}{}

			// Perform geocoding to get the location
			locations, err := GetLocation(value)
			if err != nil {
				return
			}

			fmt.Printf("Odd RowIndex: %d\n", rowIndex)

			// Update the data in columns Z and AA in the same row
			updateRowData(sheet, rowIndex, locations.Longitude, locations.Latitude)

			// Release the semaphore
			<-semaphore
		}
	}()

	// Launch goroutine for odd row indices
	wg.Add(1)
	go func() {
		defer wg.Done()
		for rowIndex, value := range values {
			// Skip the first row (titles) and even row indices
			if rowIndex == 0 || rowIndex%2 == 0 {
				continue
			}

			// Acquire a semaphore to control the concurrency
			semaphore <- struct{}{}

			// Perform geocoding to get the location
			location, err := GetLocation(value)
			if err != nil {
				return
			}

			fmt.Printf("Even RowIndex: %d\n", rowIndex)

			// Update the data in columns Z and AA in the same row
			updateRowData(sheet, rowIndex, location.Longitude, location.Latitude)

			// Release the semaphore
			<-semaphore
		}
	}()

	// Wait for both goroutines to finish
	wg.Wait()

	//Save the modified file
	err = file.Save("SdxStores_Prod_20240304_updated.xlsx")
	if err != nil {
		fmt.Printf("Error saving file: %v\n", err)
		return
	}

	fmt.Println("Data updated and saved successfully.")
}

func getColumnValues(sheet *xlsx.Sheet, column string) []string {
	// Convert the column letter to the corresponding index
	columnIndex := columnToIndex(column)

	var values []string

	// Iterate over the rows in the sheet
	for _, row := range sheet.Rows {
		// Check if the row has enough cells
		if columnIndex < len(row.Cells) {
			// Append the cell value to the values slice
			values = append(values, row.Cells[columnIndex].Value)
		}
	}

	return values
}

func columnToIndex(column string) int {
	// Convert the column letter to the corresponding index
	index := 0
	for i := 0; i < len(column); i++ {
		index = index*26 + int(column[i]-'A'+1)
	}
	return index - 1
}

func updateRowData(sheet *xlsx.Sheet, rowIndex int, lng, lat float64) {
	// Convert the column letters to corresponding indices
	columnZIndex := columnToIndex("Z")
	columnAAIndex := columnToIndex("AA")

	// Get the row at the specified index
	row := sheet.Rows[rowIndex]

	// Update the values in columns Z and AA
	if columnZIndex < len(row.Cells) {
		row.Cells[columnZIndex].Value = fmt.Sprintf("%f", lng)
	}
	if columnAAIndex < len(row.Cells) {
		row.Cells[columnAAIndex].Value = fmt.Sprintf("%f", lat)
	}
}
