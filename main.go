package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/parnurzeal/gorequest"
)

// needed structs to work with all of the data
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

// function to download the file form S3
func downloadS3() {
	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String("eu-central-1"),
		Credentials: credentials.NewSharedCredentials("", "default"),
	})

	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 // 64MB per part
		d.Concurrency = 6
	})

	f, err := os.Create("deals.csv.gz")
	if err != nil {
		log.Println("failed to create file", "deals.csv.gz", err)
	}

	// Write the contents of S3 Object to the file
	n, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String("pdw-export.zulu"),
		Key:    aws.String("test_tasks/deals.csv.gz"),
	})
	if err != nil {
		log.Println("failed to download file", "deals.csv.gz", err)
	}
	log.Printf("file downloaded, %d bytes\n", n)
}

// This function sends a get request to Pipedrive and downloads my deals // can easily be replaced by variable <domain> instead of edukoht in string, same with <api_token>, can be added to token files
func getDealsFromPipeDrive() {
	var success Success
	url := "https://edukoht.pipedrive.com/api/v1/deals?api_token=2d514988a632fcc6cc85f020c64b6f77a9dad678"
	resp, err := http.Get(url)

	if err != nil {
		log.Println("Error creating request", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err.Error())
	}
	json.Unmarshal(body, &success) //turn success data into array of structs
	var ListDeals []DealList
	for i := 0; i < len(success.Data); i++ {
		ListDeals = append(ListDeals, DealList{success.Data[i].Title, success.Data[i].Value})
	}
	readDeals(ListDeals) //call readDeals to read deals from csv and decide which values to update and post request to Pipedrive
}

// this function reads data from csv file downloaded from S3 and decide what values to update and post request to Pipedrive
func readDeals(listDeals []DealList) {
	f, err := os.Open("deals.csv.gz")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	deals := csv.NewReader(gr)
	if _, err := deals.Read(); err != nil {
		panic(err)
	}
	rows, err := deals.ReadAll()
	if err != nil {
		panic(err)
	}
	var companyDeals []Deal
	for _, deal := range rows {
		value, _ := strconv.ParseFloat(deal[2], 64)
		data := Deal{
			Title:    deal[0],
			Currency: deal[1],
			Value:    value * 2,
			Status:   deal[3],
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
			url := "https://edukoht.pipedrive.com/api/v1/deals?api_token=2d514988a632fcc6cc85f020c64b6f77a9dad678"
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
		url := "https://edukoht.pipedrive.com/api/v1/deals?api_token=2d514988a632fcc6cc85f020c64b6f77a9dad678"
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
	//downloadS3()  //We download the S3 file with deals. uncomment this for production
	getDealsFromPipeDrive() //to check what values need to be updated, we go to getDealsFromPipeDrive
}
