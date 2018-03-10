package afero

import (
	"encoding/json"

	"log"
	"os"

	// Imports the Google Cloud Storage client package.
	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

func main() {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "    ")
	enc.Encode(1)

	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Sets the name for the new bucket.
	bucketName := "zatte"

	// Creates a Bucket instance.
	bkt := client.Bucket(bucketName)

	it := bkt.Objects(ctx, &storage.Query{"", "C:/Users/Mikael/AppData/Local/Temp/afero989301429/more", false})
	for {
		object, err := it.Next()
		if err != nil {
			enc.Encode(err)
			break
		}
		enc.Encode(object)
	}
	/*
		r, _ := obj.NewReader(ctx)
		w := obj.NewWriter(ctx)

		if r != nil {

			if _, err := io.Copy(w, r); err != nil {
				println(err)
			}

			if err := r.Close(); err != nil {
				fmt.Println(err)
			}
		}
		w.Write([]byte("\nAnd a new line!"))

		if err := w.Close(); err != nil {
			fmt.Println(err)
		}


		r, _ = obj.NewReader(ctx)
		io.Copy(os.Stdout, r)
	*/

	//obj := bkt.Object("test/")
	//data, _ := obj.Attrs(ctx)

}
