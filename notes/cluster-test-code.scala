INSERT AFTER CALL TO ActionEngine.process!

      import com.cars.ingestionframework.ClusterTestUtil => CTU
      ;{
        // TODO TEMP FOR TEST
        //(affiliate_id -> 550039O)
        //(front_door_affiliate_pty_id -> 550039)
        //(EnrichedField1 -> 1)
        //(EnrichedField2 -> 2)
        println("(ALL TESTS STARTING) TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT")
        enrichedRDD.collect.foreach{ enrichedMap => 
          println("(test starting) TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT")

          // front_door_affiliate_pty_id
          CTU.checkEqualPrint(enrichedMap.get("EnrichedField1").get, "1")
          CTU.checkEqualPrint(enrichedMap.get("EnrichedField2").get, "2")
          CTU.checkEqualPrint(enrichedMap.get("affiliate_id").get, enrichedMap.get("front_door_affiliate_pty_id").get + "O")

          // web_page_type_id
          val typeId = enrichedMap.get("web_page_type_id")
          CTU.checkPrint(typeId.nonEmpty)
          CTU.checkPrint(List("2251", "2253").contains(typeId.get))
          CTU.checkEqualPrint(enrichedMap.get("EnrichedField10").get, "10")
          CTU.checkEqualPrint(enrichedMap.get("EnrichedField20").get, "20")
          def getWPTypeName(typeId: String) = typeId match {
            case "2251" => "Vehicle Impression (MMY Research)"
            case "2253" => "Dealer Impression (MMY Research)"
          }
          def getWPTypeDesc(typeId: String) = typeId match {
            case "2251" => "The vehicle impression on the MMY Research page."
            case "2253" => "The dealer impression on the MMY Research page."
          }
          CTU.checkEqualPrint(enrichedMap.get("web_page_type_name").get, getWPTypeName(typeId.get))
          CTU.checkEqualPrint(enrichedMap.get("web_page_type_desc").get, getWPTypeDesc(typeId.get))

          // other front door affiliate pty id
          CTU.checkEqualPrintOpt(enrichedMap.get("branding_name"), Some("Cars.com National"))
          CTU.checkEqualPrintOpt(enrichedMap.get("official_name"), Some("Cars.com National"))
          CTU.checkEqualPrintOpt(enrichedMap.get("EnrichedField100"), Some("100"))
          //println("EEEEEEEEEEEEEEEE enrichedMap = "+enrichedMap)

          println("(test done) TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT")
        }
        println("(ALL TESTS DONE) TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT")

        throw new Exception("TEST DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      }

