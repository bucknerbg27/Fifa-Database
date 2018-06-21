import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.apache.http.client.CookieStore
import org.apache.http.client.ResponseHandler
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.*
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.collections.ArrayList


class FUTDbScrape {
    val connect = DriverManager.getConnection("jdbc:mysql://localhost/?autoReconnect=true&useSSL=false&serverTimezone=UTC", "root", "Wwjd5406!")
    fun createDBTable() {
        try {
            var sql = "CREATE DATABASE IF NOT EXISTS FIFA18 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci"
            var stmt = connect.prepareStatement(sql)
            stmt.execute()

            val crtPlayerTble = """
            CREATE TABLE IF NOT EXISTS FIFA18.PLAYERS(
                PLAYER_ID INT NOT NULL PRIMARY KEY,
                FIRST_NAME VARCHAR(20) NOT NULL,
                LAST_NAME VARCHAR(30) NOT NULL,
                BIRTHDAY VARCHAR(12),
                COLOR VARCHAR(20),
                AGE INT,
                POSITION VARCHAR(5) NOT NULL,
                FULL_POSITION VARCHAR(30) NOT NULL,
                FOOT VARCHAR(5),
                HEIGHT INT NOT NULL,
                PLAYSTYLE VARCHAR(20) NOT NULL,
                WEIGHT INT NOT NULL,
                NATION_ID INT NOT NULL,
                LEAGUE_ID INT NOT NULL,
                CLUB_ID INT NOT NULL,
                ATTACK_WR VARCHAR(10) NOT NULL,
                DEFENSE_WR VARCHAR(10) NOT NULL,
                QUALITY VARCHAR(15) NOT NULL,
                IS_SPECIAL VARCHAR(5) NOT NULL,
                RATING INT NOT NULL,
                PACE_ATTR INT NOT NULL,
                SHOT_ATTR INT NOT NULL,
                PASS_ATTR INT NOT NULL,
                DRIBBLE_ATTR INT NOT NULL,
                PHYSICALITY_ATTR INT NOT NULL,
                DEFENSE_ATTR INT NOT NULL
            )"""
            stmt = connect.prepareStatement(crtPlayerTble)
            stmt.execute()

            val crtGoalKeeperTble = """
            CREATE TABLE IF NOT EXISTS FIFA18.GOALKEEPERS(
                PLAYER_ID INT NOT NULL PRIMARY KEY,
                FIRST_NAME VARCHAR(20) NOT NULL,
                LAST_NAME VARCHAR(30) NOT NULL,
                BIRTHDAY VARCHAR(12) NOT NULL,
                COLOR VARCHAR(20),
                AGE INT,
                POSITION VARCHAR(5) NOT NULL,
                FULL_POSITION VARCHAR(30) NOT NULL,
                FOOT VARCHAR(5),
                HEIGHT INT NOT NULL,
                PLAYSTYLE VARCHAR(20) NOT NULL,
                WEIGHT INT NOT NULL,
                NATION_ID INT NOT NULL,
                LEAGUE_ID INT NOT NULL,
                CLUB_ID INT NOT NULL,
                ATTACK_WR VARCHAR(10) NOT NULL,
                DEFENSE_WR VARCHAR(10) NOT NULL,
                QUALITY VARCHAR(15) NOT NULL,
                IS_SPECIAL VARCHAR(5) NOT NULL,
                RATING INT NOT NULL,
                DIVING_ATTR INT NOT NULL,
                HAND_ATTR INT NOT NULL,
                KICK_ATTR INT NOT NULL,
                REFLEX_ATTR INT NOT NULL,
                SPPED_ATTR INT NOT NULL,
                POSITION_ATTR INT NOT NULL
            )"""
            stmt = connect.prepareStatement(crtGoalKeeperTble)
            stmt.execute()

            val crtClubsTble = """
            CREATE TABLE IF NOT EXISTS FIFA18.CLUBS(
                CLUB_ID INT NOT NULL PRIMARY KEY,
                CLUB_ABBR VARCHAR(50) NOT NULL,
                CLUB_NAME VARCHAR(50) NOT NULL
            )"""
            stmt = connect.prepareStatement(crtClubsTble)
            stmt.execute()

            val crtNationTble = """
            CREATE TABLE IF NOT EXISTS FIFA18.NATIONS(
                NATION_ID INT NOT NULL PRIMARY KEY,
                NATION_ABBR VARCHAR(50) NOT NULL,
                NATION_NAME VARCHAR(50) NOT NULL
            )"""
            stmt = connect.prepareStatement(crtNationTble)
            stmt.execute()

            val crtLeagueTble = """
            CREATE TABLE IF NOT EXISTS FIFA18.LEAGUES(
                LEAGUE_ID INT NOT NULL PRIMARY KEY,
                LEAGUE_ABBR VARCHAR(50) NOT NULL,
                LEAGUE_NAME VARCHAR(50) NOT NULL
            )"""
            stmt = connect.prepareStatement(crtLeagueTble)
            stmt.execute()

            val crtStatsTble = """
            CREATE TABLE IF NOT EXISTS FIFA18.STATS(
                PLAYER_ID INT NOT NULL PRIMARY KEY,
                ACCELERATION INT NOT NULL,
                AGGRESSION INT NOT NULL,
                BALANCE INT NOT NULL,
                BALLCONTROLL INT NOT NULL,
                COMPOSURE INT NOT NULL,
                CROSSING INT NOT NULL,
                CURVE INT NOT NULL,
                DRIBBLING INT NOT NULL,
                FINISHING INT NOT NULL,
                FREE_KICK_ACC INT NOT NULL,
                GK_DIVING INT NOT NULL,
                GK_HANDLING INT NOT NULL,
                GK_KICKING INT NOT NULL,
                GK_POSITION INT NOT NULL,
                GK_REFLEX INT NOT NULL,
                HEADING_ACC INT NOT NULL,
                INTERCEPTIONS INT NOT NULL,
                JUMPING INT NOT NULL,
                LONG_PASS INT NOT NULL,
                LONG_SHOT INT NOT NULL,
                MARKING INT NOT NULL,
                PENALTY INT NOT NULL,
                POSITIONING INT NOT NULL,
                POTENTIAL INT NOT NULL,
                REACTION INT NOT NULL,
                SHORT_PASS INT NOT NULL,
                SHOT_POWER INT NOT NULL,
                SKILL_MOVES INT NOT NULL,
                SLIDE_TACKLE INT NOT NULL,
                SPRINT_SPEED INT NOT NULL,
                STAMINA INT NOT NULL,
                STANDING_TACKLE INT NOT NULL,
                STRENGTH INT NOT NULL,
                VISION INT NOT NULL,
                VOLLEY INT NOT NULL,
                WEAK_FOOT INT NOT NULL
            )"""
            stmt = connect.prepareStatement(crtStatsTble)
            stmt.execute()


        } catch (e: SQLException) {
            println(e.message)
            println(e.errorCode)
        }
    }

    fun playerDataAsJSON() :  ArrayList<List<Any>>{
        var httpCookieStore: CookieStore = BasicCookieStore()
        var customizedRequestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()
        var customClient = HttpClients.custom().setDefaultRequestConfig(customizedRequestConfig)
        var client: CloseableHttpClient = customClient.setRedirectStrategy(LaxRedirectStrategy()).setDefaultCookieStore(httpCookieStore).build()
        var page = 1
        var pageNum = 1
        val playersData = arrayListOf<List<Any>>()
        var jp = JsonParser()

        do {
            var httpGet = HttpGet("https://www.easports.com/fifa/ultimate-team/api/fut/item?page=" + pageNum)
            var responseHandler: ResponseHandler<String> = BasicResponseHandler()
            var response = client.execute(httpGet, responseHandler)

            var pac = 0
            var sho = 0
            var pas = 0
            var dri = 0
            var phy = 0
            var div = 0
            var han = 0
            var kick = 0
            var ref = 0
            var spd = 0
            var pos = 0
            var def = 0


            val jsonObject: JsonObject = jp.parse(response) as JsonObject
            var totalPage = jsonObject.get("totalPages")
            page = totalPage.asInt

            val playerArray: JsonArray = jsonObject.getAsJsonArray("items")
            for (i in 0 until playerArray.size()) {

                val playerData = playerArray.get(i).toString()
                val player = jp.parse(playerData) as JsonObject
                val fName = player.get("firstName").asString
                //val fName = ""
                val lName = player.get("lastName").asString
                val acc = player.get("acceleration").asInt
                val age = player.get("age").asInt
                val agg = player.get("aggression").asInt
                val atkWR = player.get("atkWorkRate").asString
                val position = player.get("position").asString
                val attrib = player.get("attributes").asJsonArray
                if (position != "GK") {
                    pac = attrib.get(0).asJsonObject.get("value").asInt
                    sho = attrib.get(1).asJsonObject.get("value").asInt
                    pas = attrib.get(2).asJsonObject.get("value").asInt
                    dri = attrib.get(3).asJsonObject.get("value").asInt
                    def = attrib.get(4).asJsonObject.get("value").asInt
                    phy = attrib.get(5).asJsonObject.get("value").asInt
                } else {
                    div = attrib.get(0).asJsonObject.get("value").asInt
                    han = attrib.get(1).asJsonObject.get("value").asInt
                    kick = attrib.get(2).asJsonObject.get("value").asInt
                    ref = attrib.get(3).asJsonObject.get("value").asInt
                    spd = attrib.get(4).asJsonObject.get("value").asInt
                    pos = attrib.get(5).asJsonObject.get("value").asInt
                }
                val bal = player.get("balance").asInt
                val ballCont = player.get("ballcontrol").asInt
                val baseID = player.get("baseId").asInt
                val bDay = player.get("birthdate").asString
                val clubAbbr = player.get("club").asJsonObject.get("abbrName").asString
                val clubID = player.get("club").asJsonObject.get("id").asInt
                val clubName = player.get("club").asJsonObject.get("name").asString
                val color = player.get("color").asString
                val cNmane = player.get("commonName").asString
                val compose = player.get("composure").asInt
                val crossing = player.get("crossing").asInt
                val curve = player.get("curve").asInt
                val defWR = player.get("defWorkRate").asString
                val dribble = player.get("dribbling").asInt
                val finish = player.get("finishing").asInt
                val foot = player.get("foot").asString
                val freekick = player.get("freekickaccuracy").asInt
                val gkDive = player.get("gkdiving").asInt
                val gkHandle = player.get("gkhandling").asInt
                val gkKick = player.get("gkkicking").asInt
                val gkPosition = player.get("gkpositioning").asInt
                val gkReflex = player.get("gkreflexes").asInt
                val headacc = player.get("headingaccuracy").asInt
                val height = player.get("height").asInt
                val id = player.get("id").asInt
                val interception = player.get("interceptions").asInt
                val isSpecial = player.get("isSpecialType").asString
                val jump = player.get("jumping").asInt
                val leagueAbbr = player.get("league").asJsonObject.get("abbrName").asString
                val leagueID = player.get("league").asJsonObject.get("id").asInt
                val leagueName = player.get("league").asJsonObject.get("name").asString
                val longPass = player.get("longpassing").asInt
                val longShot = player.get("longshots").asInt
                val marking = player.get("marking").asInt
                val name = player.get("name").asString
                val nationAbbr = player.get("nation").asJsonObject.get("abbrName").asString
                val nationID = player.get("nation").asJsonObject.get("id").asInt
                val nationName = player.get("nation").asJsonObject.get("name").asString
                val penalty = player.get("penalties").asInt
                val playerType = player.get("playerType").asString
                val playStyle = player.get("playStyle").asString
                val positonF = player.get("positionFull").asString
                val positioning = player.get("positioning").asInt
                val potential = player.get("potential").asInt
                val quality = player.get("quality").asString
                val rating = player.get("rating").asInt
                val reactions = player.get("reactions").asInt
                val shortPass = player.get("shortpassing").asInt
                val shotPwr = player.get("shotpower").asInt
                val skillMove = player.get("skillMoves").asInt
                val slideTackle = player.get("slidingtackle").asInt
                val sprintSpeed = player.get("sprintspeed").asInt
                val stamina = player.get("stamina").asInt
                val standingTackle = player.get("standingtackle").asInt
                val strength = player.get("strength").asInt
                val vision = player.get("vision").asInt
                val volley = player.get("volleys").asInt
                val weakFoot = player.get("weakFoot").asInt
                val weight = player.get("weight").asInt

//                httpGet = HttpGet("https://www.easports.com/fifa/ultimate-team/api/fut/price-band/" + id)
//                response = client.execute(httpGet, responseHandler)
//
//                val jsonObject2 = jp.parse(response) as JsonObject
//                val minPrice = jsonObject2.getAsJsonObject(id.toString()).getAsJsonObject("priceLimits").getAsJsonObject("ps4").get("minPrice")
//                val maxPrice = jsonObject2.getAsJsonObject(id.toString()).getAsJsonObject("priceLimits").getAsJsonObject("ps4").get("maxPrice")

                val players = listOf(fName, lName, acc, age,
                        agg, atkWR, position, pac, pas,
                        sho, dri, def, phy, div, han,
                        kick, ref, spd, pos, bal,
                        ballCont, baseID, bDay, clubAbbr, clubID, clubName,
                        color, cNmane, compose, crossing, curve,
                        defWR, dribble, finish, foot, freekick,
                        gkDive, gkHandle, gkKick, gkReflex, gkPosition,
                        headacc, height, id, interception, isSpecial,
                        jump, leagueAbbr, leagueID, leagueName, longPass,
                        longShot, marking, name, nationAbbr, nationID,
                        nationName, penalty, playerType, playStyle,
                        positonF, positioning, potential, quality, rating,
                        reactions, shortPass, shotPwr, skillMove, slideTackle,
                        sprintSpeed, stamina, standingTackle, strength,
                        vision, volley, weakFoot, weight)



                playersData.add(players)

            }

            pageNum++
            println("$pageNum")

        } while (pageNum <= page)

        for (i in 0 until playersData.size) {
//            if (playersData.get(i).get(6) != "GK") {
//
//                println("${playersData.get(i).get(43)}, ${playersData.get(i).get(0)}, ${playersData.get(i).get(1)}, ${playersData.get(i).get(22)}, " +
//                        "${playersData.get(i).get(26)}, ${playersData.get(i).get(3)}, ${playersData.get(i).get(6)}, ${playersData.get(i).get(60)}, ${playersData.get(i).get(34)}, " +
//                        "${playersData.get(i).get(42)}, ${playersData.get(i).get(59)}, ${playersData.get(i).get(77)}, ${playersData.get(i).get(55)}, " +
//                        "${playersData.get(i).get(46)}, ${playersData.get(i).get(24)}, ${playersData.get(i).get(5)}, ${playersData.get(i).get(31)}, " +
//                        "${playersData.get(i).get(63)}, ${playersData.get(i).get(45)}, ${playersData.get(i).get(64)}, ${playersData.get(i).get(7)}, " +
//                        "${playersData.get(i).get(8)}, ${playersData.get(i).get(9)}, ${playersData.get(i).get(10)}, ${playersData.get(i).get(11)}, " +
//                        "${playersData.get(i).get(12)}")
//            } else {
                println("${playersData.get(i).get(48)}, ${playersData.get(i).get(0)}, ${playersData.get(i).get(1)}, ${playersData.get(i).get(22)}, " +
                        "${playersData.get(i).get(26)}, ${playersData.get(i).get(3)}, ${playersData.get(i).get(6)}, ${playersData.get(i).get(60)}, ${playersData.get(i).get(34)}, " +
                        "${playersData.get(i).get(42)}, ${playersData.get(i).get(59)}, ${playersData.get(i).get(77)}, ${playersData.get(i).get(55)}, " +
                        "${playersData.get(i).get(46)}, ${playersData.get(i).get(24)}, ${playersData.get(i).get(5)}, ${playersData.get(i).get(31)}, " +
                        "${playersData.get(i).get(63)}, ${playersData.get(i).get(45)}, ${playersData.get(i).get(64)}, ${playersData.get(i).get(13)}, " +
                        "${playersData.get(i).get(14)}, ${playersData.get(i).get(15)}, ${playersData.get(i).get(16)}, ${playersData.get(i).get(17)}, " +
                        "${playersData.get(i).get(18)}")

            }
//        }
        println(playersData.size)

        return playersData

    }

    fun initializeDB() {
        val players = playerDataAsJSON()
        try {
            for (i in 0 until players.size) {
                var fName = "${players.get(i).get(0)}"
                var lName = "${players.get(i).get(1)}"
                var clubAbbr = "${players.get(i).get(23)}"
                var clubName = "${players.get(i).get(25)}"
                var nationAbbr = "${players.get(i).get(54)}"
                var nationName = "${players.get(i).get(56)}"
                var leagueId = "${players.get(i).get(48)}"
                var leagueName = "${players.get(i).get(49)}"
                if (fName.toString().contains("'")) {
                    fName = fName.replace("'","\\'")
                }
                if (lName.toString().contains("'")) {
                    lName = lName.replace("'", "\\'")
                }
                if (clubAbbr.toString().contains("'")) {
                    clubAbbr = clubAbbr.replace("'", "\\'")
                }
                if (clubName.toString().contains("'")) {
                    clubName = clubName.replace("'", "\\'")
                }
                if (nationAbbr.toString().contains("'")) {
                    nationAbbr = nationAbbr.replace("'", "\\'")
                }
                if (nationName.toString().contains("'")) {
                    nationName = nationName.replace("'", "\\'")
                }

                if (leagueId.toString().contains("'")) {
                    leagueId = leagueId.replace("'", "\\'")
                }

                if (leagueName.toString().contains("'")) {
                    leagueName = leagueName.replace("'", "\\'")
                }

                var playerSQL = """INSERT INTO FIFA18.PLAYERS
                VALUES (${players.get(i).get(43)}, '$fName', '$lName', '${players.get(i).get(22)}',
                 '${players.get(i).get(26)}', ${players.get(i).get(3)}, '${players.get(i).get(6)}', '${players.get(i).get(60)}', '${players.get(i).get(34)}',
                 ${players.get(i).get(42)}, '${players.get(i).get(59)}',
                 ${players.get(i).get(77)}, ${players.get(i).get(55)}, ${players.get(i).get(46)}, ${players.get(i).get(24)}, '${players.get(i).get(5)}',
                 '${players.get(i).get(31)}', '${players.get(i).get(63)}', '${players.get(i).get(45)}', ${players.get(i).get(64)}, ${players.get(i).get(7)},
                 ${players.get(i).get(8)}, ${players.get(i).get(9)}, ${players.get(i).get(10)}, ${players.get(i).get(11)}, ${players.get(i).get(12)})
                 ON DUPLICATE KEY UPDATE PLAYER_ID = PLAYER_ID;"""

                var keeperSQL = """ INSERT INTO FIFA18.GOALKEEPERS

                VALUES (${players.get(i).get(43)}, '$fName', '$lName', '${players.get(i).get(22)}',
                 '${players.get(i).get(26)}', ${players.get(i).get(3)}, '${players.get(i).get(6)}', '${players.get(i).get(60)}', '${players.get(i).get(34)}',
                 ${players.get(i).get(42)}, '${players.get(i).get(59)}',
                 ${players.get(i).get(77)}, ${players.get(i).get(55)}, ${players.get(i).get(48)}, ${players.get(i).get(24)}, '${players.get(i).get(5)}',
                 '${players.get(i).get(31)}', '${players.get(i).get(63)}', '${players.get(i).get(45)}', ${players.get(i).get(64)}, ${players.get(i).get(13)},
                 ${players.get(i).get(14)}, ${players.get(i).get(15)}, ${players.get(i).get(16)}, ${players.get(i).get(17)}, ${players.get(i).get(18)})
                 ON DUPLICATE KEY UPDATE PLAYER_ID = PLAYER_ID;"""

                var nationSQL = """INSERT INTO FIFA18.NATIONS
                VALUES(${players.get(i).get(55)}, '$nationAbbr', '$nationName')
                ON DUPLICATE KEY UPDATE NATION_ID = NATION_ID;"""

                var leagueSQL = """INSERT INTO FIFA18.LEAGUES
                VALUES('$leagueId', '${players.get(i).get(47)}', '$leagueName')
                ON DUPLICATE KEY UPDATE LEAGUE_ID = LEAGUE_ID;"""

                var clubSQL = """INSERT INTO FIFA18.CLUBS
                VALUES(${players.get(i).get(24)}, '$clubAbbr', '$clubName')
                ON DUPLICATE KEY UPDATE CLUB_ID = CLUB_ID;"""

                var statsSQL = """INSERT INTO FIFA18.STATS
                VALUES(${players.get(i).get(43)}, ${players.get(i).get(2)}, ${players.get(i).get(4)}, ${players.get(i).get(19)}, ${players.get(i).get(20)},
                ${players.get(i).get(28)}, ${players.get(i).get(29)}, ${players.get(i).get(30)}, ${players.get(i).get(32)},${players.get(i).get(33)},
                ${players.get(i).get(35)}, ${players.get(i).get(36)}, ${players.get(i).get(37)}, ${players.get(i).get(38)}, ${players.get(i).get(39)},
                ${players.get(i).get(40)}, ${players.get(i).get(41)}, ${players.get(i).get(44)}, ${players.get(i).get(46)}, ${players.get(i).get(50)},
                ${players.get(i).get(51)}, ${players.get(i).get(52)}, ${players.get(i).get(57)}, ${players.get(i).get(61)},
                ${players.get(i).get(62)}, ${players.get(i).get(65)}, ${players.get(i).get(66)}, ${players.get(i).get(67)}, ${players.get(i).get(68)},
                ${players.get(i).get(69)}, ${players.get(i).get(70)}, ${players.get(i).get(71)}, ${players.get(i).get(72)}, ${players.get(i).get(73)},
                ${players.get(i).get(74)}, ${players.get(i).get(75)}, ${players.get(i).get(76)})
                ON DUPLICATE KEY UPDATE PLAYER_ID = PLAYER_ID;"""


                if (players.get(i).get(6) != "GK") {

                    var stmt = connect.prepareStatement(playerSQL)
                    stmt.execute()
                } else {
                    var stmt = connect.prepareStatement(keeperSQL)
                    stmt.execute()
                }
                var stmt = connect.prepareStatement(nationSQL)
                stmt.execute()
                stmt = connect.prepareCall(clubSQL)
                stmt.execute()
                stmt = connect.prepareCall(leagueSQL)
                stmt.execute()
                stmt = connect.prepareStatement(statsSQL)
                stmt.execute()
            }
        } catch (e: SQLException) {
            println(e.message)
            println(e.errorCode)
        }

    }
}
