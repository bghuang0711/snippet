package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/Shopify/sarama"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	//targetFile = flag.String("t", "kafkax/target_001_success.json", "target")
	//targetFile = flag.String("t", "kafkax/target_002_unmatch.json", "target")
	//targetFile = flag.String("t", "kafkax/target_003_not_found.json", "target")
	//targetFile = flag.String("t", "kafkax/target_004_repeated.json", "target")
	//targetFile = flag.String("t", "kafkax/target_005_filter_trigger.json", "target")
	targetFile = flag.String("t", "kafkax/target_006_filter_recon.json", "target")
)

func main() {
	data, err := ioutil.ReadFile(*targetFile)
	if err != nil {
		log.Fatal(err)
		return
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatal(err)
		return
	}

	for _, target := range config.Targets {
		if err := ExecuteTarget(target); err != nil {
			log.Fatal(err)
		}
	}
}

/*
{
	"uuid": "test-uuid-001",
	"command": "insert",
	"table": "mall_db.order_tab",
	"timestamp_db": 0,
	"timestamp_canal": 0,
	"column_count": 0,
	"primary_keys": ["id"],
	"new_row": {
		"id": "order-id-001",
		"order_no": "order-no-001",
		"order_amount": 9999,
		"order_status": 1,
		"payment_id": "payment-id-001",
		"create_time": 0,
		"complete_time": 0
	},
	"old_row": {},
	"rows_type": {},
	"status": 0
}
*/

type ScanalBinlog struct {
	UUID           string                 `json:"uuid"`            // message's uuid
	Command        string                 `json:"command"`         // ddl type: insert, update, delete
	Table          string                 `json:"table"`           // table name, format: dbName.tableName, for example: test_db.test_tab
	TimestampDB    int64                  `json:"timestamp_db"`    // db execution timestamp
	TimestampCanal int64                  `json:"timestamp_canal"` // scanal parse timestamp
	ColumnCount    uint16                 `json:"column_count"`    // column count, for example: test_tab have column a and b and c, then ColumnCount will be 3
	PrimaryKeys    []string               `json:"primary_keys"`    // primary keys name list, for example: test_tab have primary key id, then the PrimaryKeys will be {"id"}
	NewRow         map[string]interface{} `json:"new_row"`         // new data row, insert and update command will have, key is column name
	OldRow         map[string]interface{} `json:"old_row"`         // old data row, update and delete command will have, key is column name
	RowsType       map[string]string      `json:"rows_type"`       // every column's mysql type, key is column name
	Status         uint8                  `json:"status"`          // enum value;0,1,0:have no column data oversize, 1: have column data over size, will set over size column to ""
}

type Config struct {
	Targets []Target `json:"targets"`
}

type Target struct {
	Brokers []string `json:"brokers"`
	Admin   struct {
		CreateTopics []struct {
			Name string `json:"name"`
		} `json:"create_topics"`
		DeleteTopics []struct {
			Name string `json:"name"`
		} `json:"delete_topics"`
	} `json:"admin"`
	Producer struct {
		Messages []struct {
			Topic      string                 `json:"topic"`
			Type       string                 `json:"type"`
			AnyBody    map[string]interface{} `json:"any_body"`
			ScanalBody ScanalBinlog           `json:"scanal_body"`
		} `json:"messages"`
	} `json:"producer"`
}

func ExecuteTarget(t Target) error {
	adminConfig := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(t.Brokers, adminConfig)
	if err != nil {
		return err
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}
	for name, detail := range topics {
		log.Println(name, "partitions", detail.NumPartitions, "factor", detail.ReplicationFactor)
	}

	for _, createTopic := range t.Admin.CreateTopics {
		_, ok := topics[createTopic.Name]
		if ok {
			log.Println(createTopic.Name, " has existed")
			continue
		}
		if err := admin.CreateTopic(createTopic.Name, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false); err != nil {
			return err
		}
	}

	for _, deleteTopic := range t.Admin.DeleteTopics {
		_, ok := topics[deleteTopic.Name]
		if !ok {
			log.Println(deleteTopic.Name, " does not exist")
			continue
		}
		if err := admin.DeleteTopic(deleteTopic.Name); err != nil {
			return err
		}
	}

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(t.Brokers, producerConfig)
	if err != nil {
		return err
	}
	defer producer.Close()

	for _, message := range t.Producer.Messages {
		var messageBytes []byte
		var err error
		switch message.Type {
		case "any":
			messageBytes, err = json.Marshal(message.AnyBody)
			if err != nil {
				return err
			}
		case "scanal":
			messageBytes, err = msgpack.Marshal(message.ScanalBody)
			if err != nil {
				return err
			}
		default:
			messageBytes, err = json.Marshal(message.AnyBody)
			if err != nil {
				return err
			}
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: message.Topic,
			Value: sarama.ByteEncoder(messageBytes),
		})
		if err != nil {
			return err
		}
	}
	return nil
}
