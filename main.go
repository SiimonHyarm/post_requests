package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/parnurzeal/gorequest"
)

// Deal needed structs to work with all the data
type Deal struct {
	Title    string  `json:"title"`
	Currency string  `json:"currency"`
	Value    float64 `json:"value"`
	Status   string  `json:"status"`
}

type DealList struct {
	Title string  `json:"title"`
	Value float64 `json:"value"`
}

type Success struct {
	AdditionalData struct {
		Pagination struct {
			Limit                 int64 `json:"limit"`
			MoreItemsInCollection bool  `json:"more_items_in_collection"`
			Start                 int64 `json:"start"`
		} `json:"pagination"`
	} `json:"additional_data"`
	Data []struct {
		Active          bool        `json:"active"`
		ActivitiesCount int64       `json:"activities_count"`
		AddTime         string      `json:"add_time"`
		CcEmail         string      `json:"cc_email"`
		CloseTime       interface{} `json:"close_time"`
		CreatorUserID   struct {
			ActiveFlag bool        `json:"active_flag"`
			Email      string      `json:"email"`
			HasPic     int64       `json:"has_pic"`
			ID         int64       `json:"id"`
			Name       string      `json:"name"`
			PicHash    interface{} `json:"pic_hash"`
			Value      int64       `json:"value"`
		} `json:"creator_user_id"`
		Currency               string      `json:"currency"`
		Deleted                bool        `json:"deleted"`
		DoneActivitiesCount    int64       `json:"done_activities_count"`
		EmailMessagesCount     int64       `json:"email_messages_count"`
		ExpectedCloseDate      interface{} `json:"expected_close_date"`
		FilesCount             int64       `json:"files_count"`
		FirstWonTime           interface{} `json:"first_won_time"`
		FollowersCount         int64       `json:"followers_count"`
		FormattedValue         string      `json:"formatted_value"`
		FormattedWeightedValue string      `json:"formatted_weighted_value"`
		GroupID                interface{} `json:"group_id"`
		GroupName              interface{} `json:"group_name"`
		ID                     int64       `json:"id"`
		Label                  interface{} `json:"label"`
		LastActivityDate       interface{} `json:"last_activity_date"`
		LastActivityID         interface{} `json:"last_activity_id"`
		LastIncomingMailTime   interface{} `json:"last_incoming_mail_time"`
		LastOutgoingMailTime   interface{} `json:"last_outgoing_mail_time"`
		LostReason             interface{} `json:"lost_reason"`
		LostTime               interface{} `json:"lost_time"`
		NextActivityDate       interface{} `json:"next_activity_date"`
		NextActivityDuration   interface{} `json:"next_activity_duration"`
		NextActivityID         interface{} `json:"next_activity_id"`
		NextActivityNote       interface{} `json:"next_activity_note"`
		NextActivitySubject    interface{} `json:"next_activity_subject"`
		NextActivityTime       interface{} `json:"next_activity_time"`
		NextActivityType       interface{} `json:"next_activity_type"`
		NotesCount             int64       `json:"notes_count"`
		OrgHidden              bool        `json:"org_hidden"`
		OrgID                  interface{} `json:"org_id"`
		OrgName                interface{} `json:"org_name"`
		OwnerName              string      `json:"owner_name"`
		ParticipantsCount      int64       `json:"participants_count"`
		PersonHidden           bool        `json:"person_hidden"`
		PersonID               struct {
			ActiveFlag bool `json:"active_flag"`
			Email      []struct {
				Primary bool   `json:"primary"`
				Value   string `json:"value"`
			} `json:"email"`
			Name    string `json:"name"`
			OwnerID int64  `json:"owner_id"`
			Phone   []struct {
				Primary bool   `json:"primary"`
				Value   string `json:"value"`
			} `json:"phone"`
			Value int64 `json:"value"`
		} `json:"person_id"`
		PersonName            string      `json:"person_name"`
		PipelineID            int64       `json:"pipeline_id"`
		Probability           interface{} `json:"probability"`
		ProductsCount         int64       `json:"products_count"`
		RenewalType           string      `json:"renewal_type"`
		RottenTime            interface{} `json:"rotten_time"`
		StageChangeTime       interface{} `json:"stage_change_time"`
		StageID               int64       `json:"stage_id"`
		StageOrderNr          int64       `json:"stage_order_nr"`
		Status                string      `json:"status"`
		Title                 string      `json:"title"`
		UndoneActivitiesCount int64       `json:"undone_activities_count"`
		UpdateTime            string      `json:"update_time"`
		UserID                struct {
			ActiveFlag bool        `json:"active_flag"`
			Email      string      `json:"email"`
			HasPic     int64       `json:"has_pic"`
			ID         int64       `json:"id"`
			Name       string      `json:"name"`
			PicHash    interface{} `json:"pic_hash"`
			Value      float64     `json:"value"`
		} `json:"user_id"`
		Value                 float64     `json:"value"`
		VisibleTo             string      `json:"visible_to"`
		WeightedValue         float64     `json:"weighted_value"`
		WeightedValueCurrency string      `json:"weighted_value_currency"`
		WonTime               interface{} `json:"won_time"`
	} `json:"data"`
	RelatedObjects struct {
		Person struct {
			One struct {
				ActiveFlag bool `json:"active_flag"`
				Email      []struct {
					Primary bool   `json:"primary"`
					Value   string `json:"value"`
				} `json:"email"`
				ID      int64  `json:"id"`
				Name    string `json:"name"`
				OwnerID int64  `json:"owner_id"`
				Phone   []struct {
					Primary bool   `json:"primary"`
					Value   string `json:"value"`
				} `json:"phone"`
			} `json:"1"`
		} `json:"person"`
		User struct {
			One4317453 struct {
				ActiveFlag bool        `json:"active_flag"`
				Email      string      `json:"email"`
				HasPic     int64       `json:"has_pic"`
				ID         int64       `json:"id"`
				Name       string      `json:"name"`
				PicHash    interface{} `json:"pic_hash"`
			} `json:"14317453"`
		} `json:"user"`
	} `json:"related_objects"`
	Success bool `json:"success"`
}

type Customer struct {
	ID        string
	FirstName string
	LastName  string
}

type Order struct {
	ID        string
	UserID    string
	OrderDate string
	Status    string
}

type Payment struct {
	ID            string
	OrderID       string
	PaymentMethod string
	Amount        float64
}

/*func dlDataSet() {
	url := "https://github.com/dbt-labs/jaffle-shop-classic/tree/main/seeds"
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error downloading page: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalf("Error closing response body: %v", err)
		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Error: received non-200 response code: %d", resp.StatusCode)
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		log.Fatalf("Error parsing HTML: %v", err)
	}

	var csvLinks []string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" && strings.HasSuffix(a.Val, ".csv") {
					link := strings.Replace(a.Val, "/blob", "", 1)
					csvLinks = append(csvLinks, "https://raw.githubusercontent.com"+link)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	for _, link := range csvLinks {
		resp, err := http.Get(link)
		if err != nil {
			log.Printf("Error downloading file %s: %v", link, err)
			continue
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				log.Printf("Error closing response body for %s: %v", link, err)
			}
		}(resp.Body)

		if resp.StatusCode != http.StatusOK {
			log.Printf("Error: received non-200 response code for %s: %d", link, resp.StatusCode)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading response body for %s: %v", link, err)
			continue
		}

		fileName := path.Base(link)
		err = os.WriteFile(fileName, body, 0644)
		if err != nil {
			log.Printf("Error writing to file %s: %v", fileName, err)
			continue
		}

		log.Printf("File %s downloaded successfully", fileName)
	}
}*/

// This function sends a get request to Pipedrive and downloads my deals // can easily be replaced by variable <domain> instead of edukoht in string, same with <api_token>, can be added to token files
func getDealsFromPipeDrive() {
	var success Success
	url := "https://test-comp-pd-task.pipedrive.com/api/v1/deals?api_token="
	resp, err := http.Get(url)

	if err != nil {
		log.Println("Error creating request", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err.Error())
	}
	err = json.Unmarshal(body, &success)
	if err != nil {
		return
	} //turn success data into array of structs
	var ListDeals []DealList
	for i := 0; i < len(success.Data); i++ {
		ListDeals = append(ListDeals, DealList{success.Data[i].Title, success.Data[i].Value})
	}
	log.Println(ListDeals)
	readDeals(ListDeals) //call readDeals to read deals from csv and decide which values to update and post request to Pipedrive
}

// this function reads data from csv file downloaded from S3 and decide what values to update and post request to Pipedrive
func readDeals(listDeals []DealList) {
	f, err := os.Open("deals.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	deals := csv.NewReader(f)
	if _, err := deals.Read(); err != nil {
		panic(err)
	}
	rows, err := deals.ReadAll()
	if err != nil {
		panic(err)
	}
	var companyDeals []Deal
	for _, record := range rows {
		amount, _ := strconv.ParseFloat(record[8], 64)
		data := Deal{
			Title:    record[2] + " " + record[1], // LastName FirstName as Title
			Currency: "EUR",                       // Default Currency
			Value:    amount,
			Status:   record[5], // Order Status
		}
		companyDeals = append(companyDeals, data)
	}
	var leng int
	var dealsToPost []Deal
	if len(companyDeals) > len(listDeals) { // generally find the shorter array of structs to avaid out of bounds errors,  then ignore values that are already updated in Pipedrive and add everything else to the array of structs
		shorter := listDeals
		leng = len(shorter)
		for i := range shorter {
			if listDeals[i].Title == companyDeals[i].Title && listDeals[i].Value == companyDeals[i].Value || companyDeals[i].Status == "deleted" {
				continue
			} else {
				dealsToPost = append(dealsToPost, Deal{companyDeals[i].Title, companyDeals[i].Currency, companyDeals[i].Value, companyDeals[i].Status})
			}
		}
	} else {
		shorter := companyDeals
		for i := range shorter {
			if listDeals[i].Title == companyDeals[i].Title && listDeals[i].Value == companyDeals[i].Value || companyDeals[i].Status == "deleted" {
				continue
			} else {
				dealsToPost = append(dealsToPost, Deal{companyDeals[i].Title, companyDeals[i].Currency, companyDeals[i].Value, companyDeals[i].Status})
			}
		}
	}
	for j := leng; j < len(companyDeals); j++ {
		dealsToPost = append(dealsToPost, Deal{companyDeals[j].Title, companyDeals[j].Currency, companyDeals[j].Value, companyDeals[j].Status})
	}
	postDeals(dealsToPost) // send the array with values missing in Pipedrive or in need of update.
}

func readAndMergeCSVFiles(dir string, outputFile string) {
	// Maps to store data from each CSV file
	customers := make(map[string]Customer)
	orders := make(map[string]Order)
	payments := make(map[string]Payment)

	// Read all CSV files in the directory
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".csv" {
			file, err := os.Open(path)
			if err != nil {
				log.Fatalf("Error opening file %s: %v", path, err)
			}
			defer file.Close()

			reader := csv.NewReader(file)
			records, err := reader.ReadAll()
			if err != nil {
				log.Fatalf("Error reading file %s: %v", path, err)
			}

			switch filepath.Base(path) {
			case "raw_customers.csv":
				for _, record := range records[1:] { // Skip headers
					customers[record[0]] = Customer{
						ID:        record[0],
						FirstName: record[1],
						LastName:  record[2],
					}
				}
			case "raw_orders.csv":
				for _, record := range records[1:] { // Skip headers
					orders[record[0]] = Order{
						ID:        record[0],
						UserID:    record[1],
						OrderDate: record[2],
						Status:    record[3],
					}
				}
			case "raw_payments.csv":
				for _, record := range records[1:] { // Skip headers
					amount, _ := strconv.ParseFloat(record[3], 64)
					payments[record[0]] = Payment{
						ID:            record[0],
						OrderID:       record[1],
						PaymentMethod: record[2],
						Amount:        amount,
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking through directory: %v", err)
	}

	// Open the output file
	outFile, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// Write headers
	headers := []string{"CustomerID", "FirstName", "LastName", "OrderID", "OrderDate", "Status", "PaymentID", "PaymentMethod", "Amount"}
	if err := writer.Write(headers); err != nil {
		log.Fatalf("Error writing headers to output file: %v", err)
	}

	// Write merged data to the output file
	for _, order := range orders {
		customer := customers[order.UserID]
		for _, payment := range payments {
			if payment.OrderID == order.ID {
				record := []string{
					customer.ID, customer.FirstName, customer.LastName,
					order.ID, order.OrderDate, order.Status,
					payment.ID, payment.PaymentMethod, strconv.FormatFloat(payment.Amount, 'f', 2, 64),
				}
				if err := writer.Write(record); err != nil {
					log.Fatalf("Error writing record to output file: %v", err)
				}
			}
		}
	}
}

func readDealsFromCSV(filePath string) []Deal {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Error closing file: %v", err)
		}
	}(file)

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Error reading CSV file: %v", err)
	}

	var deals []Deal
	for _, record := range records[1:] { // Skip headers
		amount, _ := strconv.ParseFloat(record[8], 64)
		deal := Deal{
			Title:    record[2] + " " + record[1], // LastName FirstName as Title
			Currency: "EUR",                       // Default Currency
			Value:    amount,
			Status:   record[5], // Order Status
		}
		deals = append(deals, deal)
	}

	return deals
}

// func post deals that divides the long array of structs into smaller slices of 130 elements per slice, thus flying just a little under 80 post request per second, hopefully slowly enough
func postDeals(deals []Deal) {
	var sliced [][]Deal
	size := 130
	var j int
	for i := 0; i < len(deals); i += size {
		j += size
		if j > len(deals) {
			j = len(deals)
		}
		sliced = append(sliced, deals[i:j])
	}

	start := time.Now()

	var wg sync.WaitGroup // waitgroup to see if all the go routines are done
	for _, deals := range sliced {
		log.Println(deals)
		wg.Add(1)
		go func(deals []Deal) { //go routines to make post requests faster, sadly I am still throttled, hopefully in case of hundres of CPUs on AWS, no throttles will be there when sending data
			url := "https://edukoht.pipedrive.com/api/v1/deals?api_token="
			for _, deal := range deals {
				request := gorequest.New()
				resp, _, err := request.Post(url).
					Send(deal).
					End()
				if err != nil {
					log.Println("Error creating request", err)
				}
				if resp.StatusCode != 200 {
					log.Println(resp.StatusCode)
					continue
				}
			}
			wg.Done()
		}(deals)
		wg.Wait()
	}
	//function to post values one by one, for some reason on my connection is slow, maybe due to limitations of indirect connection, thus commented out
	/* 	for _, deal := range deals {
		log.Println("deal", deal)
		request := gorequest.New()
		url := "https://edukoht.pipedrive.com/api/v1/deals?api_token="
		resp, _, err := request.Post(url).
			Send(deal).
			End()
		if err != nil {
			log.Println("Error creating request", err)
		}
		if resp.StatusCode != 201 {
			log.Println(resp.StatusCode)
			continue
		}
	} */
	log.Println("Execution Time: ", time.Since(start).Seconds()) //find the time elapsed on the upload
	//wg.Wait()
}

func main() {
	//dlDataSet() //download the file from the link
	getDealsFromPipeDrive() //to check what values need to be updated, we go to getDealsFromPipeDrive
	// readAndMergeCSVFiles(".", "deals.csv") //merge all the csv files in the directory
}
