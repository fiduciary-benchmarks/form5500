package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	utils "github.com/fiduciary-benchmarks/form5500/internal/utils"
)

const form5500Search string = "form_5500_search"

func rebuildSearchTable(section string, years []string, rkMappingFile string) {
	fmt.Println("Building form_5500_search table...")

	for _, statement := range getRebuildStatements(section, years, rkMappingFile) {
		statement.Exec()
	}
}

func findUnmatchedRks(jiraCreator string, jiraToken string, jiraAssignee string) {
	rows, err := getUnmatchedRksStatement().Query()
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	f, err := os.Create("unmatched_rks.csv")
	defer f.Close()
	fmt.Fprintln(f, "rk_name, possible_match, company_id, similarity")
	for rows.Next() {
		var name, match_name string
		var match_id, match_score string
		scErr := rows.Scan(&name, &match_name, &match_id, &match_score)
		if scErr != nil {
			fmt.Printf("error scanning %v", scErr)
			return
		}
		str := fmt.Sprintf("%v,%v,%v,%v", name, match_name, match_id, match_score)
		fmt.Fprintln(f, strings.Replace(str, "-1", "", -1))
	}
	utils.CreateJiraIssue(jiraCreator, jiraToken, jiraAssignee)
}

//private

func getRebuildStatements(section string, years []string, rkMappingFile string) []utils.SQLRunner {
	var executableStatements []utils.SQLRunner

	for _, statement := range getDropAndCreateSearchTableStatements() {
		executableStatements = append(executableStatements, statement)
	}

	var unionTables []string

	for _, year := range years {
		unionTables = append(unionTables, selectLongFormTable(year, section))
		unionTables = append(unionTables, selectShortFormTable(year, section))
	}

	selectStatement := strings.Join(unionTables, "\n      UNION ALL\n")
	cols := ""
	for _, row := range utils.TableMappings() {
		cols += row.Alias + ","
	}

	cols += "table_origin"

	insertStatement := fmt.Sprintf("INSERT INTO form_5500_search (%[1]s) SELECT %[1]s FROM (\n%[2]s\n) as f_s;", cols, selectStatement)
	executableStatements = append(executableStatements, utils.SQLRunner{Statement: insertStatement, Description: "Inserting records into form_5500_search"})

	// - Set total assets on form_5500_search from schedule H, or I
	// - Set providers on form_5500_search from schedule C if applicable (long form only)
	//   based on service codes http://freeerisa.benefitspro.com/static/popups/legends.aspx#5500c09
	for _, year := range years {
		for _, statement := range getUpdateFromSchedulesStatements(section, year) {
			executableStatements = append(executableStatements, statement)
		}
	}

	//remove junk rows
	executableStatements = append(executableStatements, getRemoveNoAssetRecords())

	//rebuild the rebuild sched_c_provider_to_fbi_rk_company_id_mappings table but only if we were given a good file
	err := validateCsvFile(rkMappingFile)
	if err != nil {
		fmt.Println("could not find rk mapping file")
	} else {
		//rebuild sched_c_provider_to_fbi_rk_company_id_mappings table
		for _, statement := range getDropAndCreateRkMappingTableStatements() {
			executableStatements = append(executableStatements, statement)
		}

		//populate sched_c_provider_to_fbi_rk_company_id_mappings table
		importRkMappings(rkMappingFile)
	}
	//update rk_mappings
	executableStatements = append(executableStatements, getUpdateRkMappings())

	// - Create materialized view form5500_search_view
	executableStatements = append(executableStatements, getCreateMaterializedViewStatement())

	// - Create index for each column in form5500_search_view
	for _, row := range utils.TableMappings() {
		executableStatements = append(executableStatements, getCreateIndexStatement(row))
	}
	return executableStatements
}

func getCreateIndexStatement(mapping utils.Mapping) utils.SQLRunner {
	return utils.SQLRunner{
		Statement:   fmt.Sprintf("CREATE INDEX %[1]s ON form5500_search_view (%[2]s);", mapping.IndexName(), mapping.Alias),
		Description: fmt.Sprintf("Creating index %[1]s", mapping.IndexName()),
	}
}

func getDropAndCreateSearchTableStatements() []utils.SQLRunner {
	var statements []utils.SQLRunner
	statements = append(statements, utils.SQLRunner{Statement: fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", form5500Search), Description: "drop form5500_search table"})
	statements = append(statements, utils.SQLRunner{Statement: fmt.Sprintf("CREATE TABLE %s (%s);", form5500Search, getSearchTableColumns()), Description: "create form5500_search table"})
	return statements
}

func getSearchTableColumns() string {
	var cols string
	for _, row := range utils.TableMappings() {
		cols += fmt.Sprintf("%s %s, ", row.Alias, row.DataType)
	}

	cols += "rk_company_id int, "
	var providerCols = []string{
		"rk_name", "rk_ein", "tpa_name", "tpa_ein", "advisor_name", "advisor_ein",
	}
	for _, col := range providerCols {
		cols += col + " text,"
	}

	var investmentTypes = []string{
		"inv_collective_trusts",
		"inv_separate_accounts",
		"inv_mutual_funds",
		"inv_general_accounts",
		"inv_company_stock",
	}
	for _, col := range investmentTypes {
		cols += col + " boolean,"
	}

	cols += "table_origin text"
	return cols
}

func selectLongFormTable(year string, section string) string {
	statement := "   SELECT "
	for _, row := range utils.TableMappings() {
		statement += fmt.Sprintf("%s as %s, ", row.LongForm, row.Alias)
	}
	statement += fmt.Sprintf("'%[1]s_%[2]s' as table_origin from f_5500_%[1]s_%[2]s as f_%[1]s", year, section)
	return statement
}

func selectShortFormTable(year string, section string) string {
	statement := "   SELECT "
	for _, row := range utils.TableMappings() {
		statement += fmt.Sprintf("%s as %s, ", row.ShortForm, row.Alias)
	}
	statement += fmt.Sprintf("'sf_%[1]s_%[2]s' as table_origin from f_5500_sf_%[1]s_%[2]s as f_%[1]s_sf", year, section)
	return statement
}

func getUnmatchedRksStatement() utils.SQLRunner {
	return utils.SQLRunner{
		Statement: fmt.Sprintf(`DROP TABLE IF EXISTS unmatched_rks;
		CREATE TEMP TABLE unmatched_rks(rk_name text);
		INSERT INTO unmatched_rks ( SELECT DISTINCT ( rk_name ) FROM form5500_search_view WHERE rk_name IS NOT NULL AND rk_company_id IS NULL );

		DROP TABLE IF EXISTS match_options;
		CREATE TEMP TABLE match_options ( rk_name text, sched_c_provider_name text, company_id int, lev int );
		INSERT INTO match_options(
				SELECT rk_name, sched_c_provider_name, fbi_company_id, levenshtein ( rk_name, sched_c_provider_name )
				FROM unmatched_rks
				LEFT JOIN sched_c_provider_to_fbi_rk_company_id_mappings
				ON LEFT ( rk_name, 2 ) = LEFT ( sched_c_provider_to_fbi_rk_company_id_mappings.sched_c_provider_name, 2 )
		);

		SELECT DISTINCT ON (match.rk_name)
					 match.rk_name, COALESCE ( sched_c_provider_name,'' ) possible_match_name, COALESCE ( company_id, -1 ) possible_match_id,  COALESCE ( match_options.lev, -1 ) match_similarity
				FROM
		(
				SELECT rk_name, min(lev) lev
				FROM match_options
				GROUP by rk_name
				) match
				LEFT JOIN match_options ON match.rk_name=match_options.rk_name AND match.lev=match_options.lev AND match.lev < 6;

		DROP TABLE IF EXISTS unmatched_rks;
		DROP TABLE IF EXISTS match_options;`),
		Description: fmt.Sprintf("Finding unmatched rks and suggested matches"),
	}
}

func getDropAndCreateRkMappingTableStatements() []utils.SQLRunner {
	var statements []utils.SQLRunner
	statements = append(statements, utils.SQLRunner{Statement: "DROP TABLE IF EXISTS sched_c_provider_to_fbi_rk_company_id_mappings;", Description: "drop sched_c_provider_to_fbi_rk_company_id_mappings table"})
	statements = append(statements, utils.SQLRunner{Statement: "CREATE TABLE sched_c_provider_to_fbi_rk_company_id_mappings ( sched_c_provider_name text PRIMARY KEY, fbi_company_id INTEGER NOT NULL);", Description: "create sched_c_provider_to_fbi_rk_company_id_mappings table"})
	return statements
}

func getUpdateRkMappings() utils.SQLRunner {
	return utils.SQLRunner{Statement: `UPDATE  form_5500_search
	SET rk_company_id = fbi_company_id
	FROM sched_c_provider_to_fbi_rk_company_id_mappings
	WHERE rk_name=sched_c_provider_name`,
		Description: "Updating records with new rk mappings"}
}

func importRkMappings(fname string) ([]utils.SQLRunner, error) {
	var statements []utils.SQLRunner
	file, err := os.Open(fname)
	if err != nil {
		return statements, fmt.Errorf("Error opening file: %v", err)
	}
	defer file.Close()
	bufReader := bufio.NewReader(file)
	csvReader := csv.NewReader(bufReader)
	var sql, errMsg string
	lines := 0
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return statements, fmt.Errorf("Error reading file: %v", err)
		}
		if len(line) != 2 {
			return statements, fmt.Errorf("Unexpected line: %v", line)
		}
		if line[0] == "" && line[1] == "" { //quit when we find an empty line
			errMsg = "Found a line with no data, stopping reading now, repair your file if data was truncated."
			break
		}
		name := strings.Replace(line[0], "'", "''", -1)
		id, _ := strconv.Atoi(line[1])
		if id != 0 { //conversion returns 0 if it's not a number, probably the header line, and in any case not valid
			sql = fmt.Sprintf("INSERT INTO sched_c_provider_to_fbi_rk_company_id_mappings (sched_c_provider_name, fbi_company_id) VALUES ('%v',%d);", name, id)
			statements = append(statements, utils.SQLRunner{Statement: sql, Description: "Importing rk company id mapping"})
			lines++
		}
	}
	if errMsg != "" {
		return statements, errors.New(errMsg)
	}
	return statements, nil
}

func validateCsvFile(fname string) error {
	file, err := os.Open(fname)
	if err != nil {
		return fmt.Errorf("Error opening file: %v", err)
	}
	defer file.Close()
	bufReader := bufio.NewReader(file)
	csvReader := csv.NewReader(bufReader)
	_, err = csvReader.Read()
	if err != nil {
		return fmt.Errorf("Error reading file: %v", err)
	}
	return nil
}
