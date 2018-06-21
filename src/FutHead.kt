import org.apache.http.client.CookieStore
import org.apache.http.client.ResponseHandler
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.BasicCookieStore
import org.apache.http.impl.client.BasicResponseHandler
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy
import org.jsoup.Jsoup


class FutHeadScrape() {
    fun getPlayerUrls() {
        var page= 1;
        var list = ArrayList<String>()
        var httpCookieStore: CookieStore = BasicCookieStore()
        var customizedRequestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()
        var customClient = HttpClients.custom().setDefaultRequestConfig(customizedRequestConfig)
        var client = customClient.setRedirectStrategy(LaxRedirectStrategy()).setDefaultCookieStore(httpCookieStore).build()


        do {

            var httpGet = HttpGet("http://www.futhead.com/18/players/?page=" + page + "&bin_platform=ps")
            var responseHandler: ResponseHandler<String> = BasicResponseHandler()
            var response = client.execute(httpGet, responseHandler)

            var doc = Jsoup.parse(response)
            var numOfPages = doc.select("span.font-12.font-bold.margin-l-r-10")
            val pageNumMax = numOfPages[0].toString()
            val IntPageNumMax = pageNumMax.substring(pageNumMax.indexOf("of")+3, pageNumMax.indexOf("</") -1).toInt()
            var players = doc.select("a.display-block.padding-0")
            var playerDoc = Jsoup.parse(players.toString())
            var elements = playerDoc.getElementsByAttribute("href")
            var attribute = elements.eachAttr("href")

            for(item in attribute) {
                list.add(item)
            }

            page++

        }while (page <= IntPageNumMax)
        println(list.size)
        for (item in list) {

            println(item)
        }

    }

    fun getPlayerData(){
        
    }
}