INSERT AFTER CALL TO ENRICH!

      // TODO TEMP FOR TEST
      //(affiliate_id -> 550039O)
      //(front_door_affiliate_pty_id -> 550039)
      //(EnrichedField1 -> 1)
      //(EnrichedField2 -> 2)
      enrichedRDD.collect.foreach{ enrichedMap => 
        println("(test starting) @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

        // front_door_affiliate_pty_id
        checkEqualPrint(enrichedMap.get("EnrichedField1").get, "1")
        checkEqualPrint(enrichedMap.get("EnrichedField2").get, "2")
        checkEqualPrint(enrichedMap.get("affiliate_id").get, enrichedMap.get("front_door_affiliate_pty_id").get + "O")

        // web_page_type_id
        val typeId = enrichedMap.get("web_page_type_id")
        checkPrint(typeId.nonEmpty)
        checkPrint(List("2251", "2253").contains(typeId.get))
        checkEqualPrint(enrichedMap.get("EnrichedField10").get, "10")
        checkEqualPrint(enrichedMap.get("EnrichedField20").get, "20")
        def getWPTypeName(typeId: String) = typeId match {
          case "2251" => "Vehicle Impression (MMY Research)"
          case "2253" => "Dealer Impression (MMY Research)"
        }
        def getWPTypeDesc(typeId: String) = typeId match {
          case "2251" => "The vehicle impression on the MMY Research page."
          case "2253" => "The dealer impression on the MMY Research page."
        }
        checkEqualPrint(enrichedMap.get("web_page_type_name").get, getWPTypeName(typeId.get))
        checkEqualPrint(enrichedMap.get("web_page_type_desc").get, getWPTypeDesc(typeId.get))

        // other front door affiliate pty id
        checkEqualPrint(enrichedMap.get("branding_name").get, "Cars.com National")
        checkEqualPrint(enrichedMap.get("official_name").get, "Cars.com National")
        checkPrint(enrichedMap.get("EnrichedField100").nonEmpty)
        //checkEqualPrint(enrichedMap.get("EnrichedField100").get, "100")

        println("(test done) @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      }
      println("(ALL TESTS DONE) @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

      throw new Exception("TEST DONE!!!!!!!!!!!!!!!!!!!!!!!!!!!")


