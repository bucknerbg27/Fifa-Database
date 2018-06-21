fun main(args : Array<String>){
    val scrape: FUTDbScrape = FUTDbScrape()

    scrape.createDBTable()
    scrape.initializeDB()
}