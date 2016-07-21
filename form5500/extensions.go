package main

import (
	"fmt"
	"log"
  "os"
  "net/http"
  "strings"
  "path/filepath"
  "io"
	utils "github.com/jdcalvin/form5500-data-sets-import/form5500/internal/utils"
)

func callExtension(extension string) {
	if (extension == "zip_codes") {
		fmt.Println("Adding zip codes extension")
		createZipCodeTable := utils.SQLRunner{
														Sql:          utils.ReadFile("sql/zip_codes/create_table.sql"),
														Description:  "Create zip_codes table",
													}

		importZipCode := utils.SQLRunner{
												Sql: fmt.Sprintf(`\copy zip_codes FROM '%s' DELIMITER ',' CSV HEADER`, downloadZipCodeCsv()),
												Description: "Importing zip codes into zip_codes table",
											}

		createZipCodeFunction := utils.SQLRunner{
															Sql:          utils.ReadFile("sql/zip_codes/create_search_function.sql"),
															Description:  "Create zip code search function",
														}

		createZipCodeTable.Exec()

		importZipCode.ExecCLI()
		createZipCodeFunction.Exec()

	} else {
		log.Fatal("Invalid extension")
	}
}

//private

func downloadZipCodeCsv() string {
	url := "https://raw.githubusercontent.com/jdcalvin/form5500-data-sets-import/master/form5500/zipcode.csv"
	tokens := strings.Split(url, "/")
	fileName := tokens[len(tokens)-1]
	fmt.Println("  - Downloading", url, "to", fileName)

	// TODO: check file existence first with io.IsExist
	output, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error while creating", fileName, "-", err)
	}
	defer output.Close()

	response, err := http.Get(url)
	if err != nil {
		fmt.Println("Error while downloading", url, "-", err)
	}
	defer response.Body.Close()

	n, err := io.Copy(output, response.Body)
	if err != nil {
		fmt.Println("Error while downloading", url, "-", err)
		fmt.Println(n)
	}

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
    fmt.Println(dir)
		log.Fatal(err)
	}
	return fmt.Sprintf("%s/%s", dir, fileName)
}


