
use anyhow::{Context, Result};
use futures::TryStreamExt;
use mongodb::{Client, bson::{Document, doc}, options::{ClientOptions, FindOptions}};

macro_rules! here {
    () => {
        // src/bin/mongodb.rs:16:5
        concat!("at ", file!(), ":", line!(), ":", column!())
    };
}


async fn run_case1(client: &Client) -> Result<()> {
    {
        // Get a handle to a database.
        let db = client.database("mydb");

        // List the names of the collections in that database.
        let collection_names = db.list_collection_names(None).await.context(here!())?;
        println!("collection_names: {}", collection_names.len());
        for collection_name in collection_names {
            println!("  - [{}]", collection_name);
        }

        // Get a handle to a collection in the database.
        let collection = db.collection::<Document>("books");

        let docs = vec![
            doc! { "_id":101i64, "title": "1984", "author": "George Orwell" },
            doc! { "_id":"202", "title": "Animal Farm", "author": "George Orwell" },
            doc! { "_id":"303", "title": "The Great Gatsby", "author": "F. Scott Fitzgerald" },
        ];

        // Insert some documents into the "mydb.books" collection.
        collection.insert_many(docs, None).await.context(here!())?;

        // Query the books in the collection with a filter and an option.
        let filter = doc! { "author": "George Orwell" };
        let find_options = FindOptions::builder().sort(doc! { "title": -1 }).build();
        let mut cursor = collection.find(filter, find_options).await.context(here!())?;

        // Iterate over the results of the cursor.
        println!("books: ");
        while let Some(book) = cursor.try_next().await.context(here!())? {
            println!("- title: {:?}", book.get("title"));
        }
    }
    Ok(())
}

async fn run_case2(client: &Client) -> Result<()> {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Book {
        _id: u64,
        title: String,
        author: String,
        payload: Vec<u8>,
    }

    // Get a handle to a database.
    let db = client.database("mydb");

    // Get a handle to a collection of `Book`.
    let typed_collection = db.collection::<Book>("books");

    let books = vec![
        Book {
            _id: 1,
            title: "The Grapes of Wrath".to_string(),
            author: "John Steinbeck".to_string(),
            payload: vec![1;8],
        },
        Book {
            _id: 2,
            title: "To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
            payload: vec![2;8],
        },
    ];

    // Insert the books into "mydb.books" collection, no manual conversion to BSON necessary.
    typed_collection.insert_many(books, None).await?;

    // Query the books in the collection with a filter and an option.
    let filter = doc! { "author": "Harper Lee" };
    // let filter = doc! { "_id": 1 };
    let find_options = FindOptions::builder().sort(doc! { "title": 1 }).build();
    let mut cursor = typed_collection.find(filter, find_options).await.context(here!())?;

    // Iterate over the results of the cursor.
    println!("books: ");
    while let Some(book) = cursor.try_next().await.context(here!())? {
        println!("  - {:?}", book);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()>{
    // Parse a connection string into an options struct.
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await.context(here!())?;
    // panic!("aaaaaaa");
    // Manually set an option.
    client_options.app_name = Some("My App".to_string());

    // Get a handle to the deployment.
    let client = Client::with_options(client_options).context(here!())?;

    {
        // List the names of the databases in that deployment.
        let db_names = client.list_database_names(None, None).await.context(here!())?;
        println!("list_database_names: {}", db_names.len());
        for db_name in  db_names{
            println!("  - [{}]", db_name);
        }
    }

    run_case1(&client).await.context(here!())?;

    run_case2(&client).await.context(here!())?;

    Ok(())
}