package AlgorithmsAndDataStructures

import scala.annotation.tailrec

/** Exercise 1.1 Internet Advertisement Hits Analysis
 *
 *  You  are  in  charge  of  an  advertising  program.
 *  Your  advertisements  are  displayed on websites all over the Internet.
 *  You have some CSV input data that counts how many times you showed an advertisement on each individual domain.
 *  Every line consists of a count and a domain name. The data looks like counts, above.
 *  Write a function that takes this input as a parameter and returns a data structure contain-ing the number of hits
 *  that were recorded on each domain and each domain under it.
 *  For example, an impression on mail.yahoo.com counts for mail.yahoo.com, ya-hoo.com, and com.
 *  Sub domains are added to the left of their parent domains.
 *  So mobile.sports.yahoo and mail are not valid domains.
 */
object InternetAdvertisement {

  def analyseImpressions(data: Array[String]): Array[String] = {
    /**
     * Take raw data.
     * Split it into counter (a number of impressions), and a domain for each record.
     * Since split() returns an array, deconstruct its components, and wrap into a tuple each.
     * Output: an array of tuples instead of array of arrays
     */
    val tupledData: Array[(Int, String)] = data.map(_.split(",")).map {
      case Array(counter, domain) => (counter.toInt, domain)
    }

    /**
     * Take a domain name (i.e. "mobile.sports.yahoo.com").
     * Split it to array of strings by a '.' symbol (i.e. Array("mobile", "sports", "yahoo", "com")).
     * If resulted array has more than 2 elements, pass the array's tail ("sports", "yahoo", "com") to a single string,
     * and pass it as a new domain to the very same function along with saving this result to the buffer list.
     * Else take the last part of the domain (i.e. in "google.com" it's "com"),
     * concatenate it with the saved, if any, results in buffer, and return it as an array
     */
    @tailrec
    def deconstructDomain(domain: String, buffer: List[String] = Nil): Array[String] = {
      val splitDomain: Array[String] = domain.split("\\.")

      if (splitDomain.length <= 2)
        (splitDomain.last :: buffer).toArray
      else {
        val tail = splitDomain.tail.mkString(".")
        deconstructDomain(tail, tail :: buffer)
      }
    }

    /**
     * Deconstruct each given domain and convert the resulted array to set to avoid duplicates
     * (i.e. both "yahoo.com" and "sports.yahoo.com" will return "com")
     */
    val deconstructedDomains: Set[String] = tupledData.flatMap(t => deconstructDomain(t._2)).toSet

    /**
     * Check each element in tupledData if it ends with a given domain.
     * If it is - increase the counter for the domain by the number
     * on this element's counter.
     * Return a tuple with newly formed counter and the domain
     */
    def countImpressionsPerDomain(domain: String): (Int, String) = {
      var counter = 0
      tupledData.foreach {
        t: (Int, String) => if (t._2.endsWith(domain)) counter += t._1
      }

      (counter, domain)
    }

    /**
     * Perform hits per domain analysis,
     * and pack it into an array sorted by descending order
     */
    val analysis = (for {
      (counter, domain) <- deconstructedDomains.map(d => countImpressionsPerDomain(d))
    } yield (counter, domain)).toArray.sorted.reverse

    /** Return computed data in the same format it was provided in */
    analysis.map(a => s"${a._1.toString},${a._2}")
  }
}

// Uncomment the following section to test algorithm
/*
object Main extends App {

  val csvData = Array(
    "900,google.com",
    "60,mail.yahoo.com",
    "10,mobile.sports.yahoo.com",
    "40,sports.yahoo.com",
    "10,stackoverflow.com",
    "2,en.wikipedia.org",
    "1,es.wikipedia.org",
    "1,mobile.sports")

  InternetAdvertisement.analyseImpressions(csvData).foreach(println)
}
*/