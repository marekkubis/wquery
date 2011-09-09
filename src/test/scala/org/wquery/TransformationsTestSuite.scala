package org.wquery
import org.testng.annotations.Test

class TransformationsTestSuite extends WQueryTestSuite {    
  @Test def testSynsetsPos() = result of ("last({}.pos)") should equal ("a\nn\ns\nv\n")

  @Test def testEmptyDataSetTransformation() = result of ("{aaaaaa}.hypernym") should equal ("(no result)\n")

  @Test def testSynsetsHypernymsCount() = result of ("count({}.hypernym)") should equal ("70\n")

  @Test def testSynsetsHypernymsHypernymCount() = result of ("count({}.hypernym.hypernym)") should equal ("68\n")

  @Test def testSynsetsHypernymsHypernymCountByComposition() = result of ("count({}.(hypernym.hypernym))") should equal ("68\n")

  @Test def testSynsetsHyponymsCount() = result of ("count({}.^hypernym)") should equal ("70\n")

  @Test def testPerson1nSynsetDesc() = result of ("{person:1:n}.desc") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } desc 'a human being; \"there was too much for one person to do\"'\n")

  @Test def testPerson1nSynsetDescFullProjection() = result of ("{person:1:n}.source^desc^destination") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } desc 'a human being; \"there was too much for one person to do\"'\n")

  @Test def testCarHypernymsSourceChangedProjection() = result of ("{car}.destination^hypernym") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } destination^hypernym^destination { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")  

  @Test def testPersonHypernymsDestinationChangedProjection() = result of ("{person}.hypernym^source") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } source^hypernym^source { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n{ person:2:n } source^hypernym^source { person:2:n }\n{ person:3:n } source^hypernym^source { person:3:n }\n")            
  
  @Test def testCarHyponymsFullProjection() = result of ("{car}.destination^hypernym^source") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { bus:4:n jalopy:1:n heap:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { compact:3:n compact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { convertible:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { coupe:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { electric:1:n electric automobile:1:n electric car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { gas guzzler:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hardtop:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hatchback:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { horseless carriage:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hot rod:1:n hot-rod:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { jeep:1:n landrover:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { loaner:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minivan:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Model T:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { pace car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { roadster:1:n runabout:1:n two-seater:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sports car:1:n sport car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Stanley Steamer:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { stock car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { subcompact:1:n subcompact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { touring car:1:n phaeton:1:n tourer:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { used-car:1:n secondhand car:1:n }\n")  
  
  @Test def testCarHyponymsBriefProjection() = result of ("{car}.^hypernym") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { bus:4:n jalopy:1:n heap:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { compact:3:n compact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { convertible:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { coupe:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { electric:1:n electric automobile:1:n electric car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { gas guzzler:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hardtop:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hatchback:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { horseless carriage:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hot rod:1:n hot-rod:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { jeep:1:n landrover:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { loaner:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minivan:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Model T:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { pace car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { roadster:1:n runabout:1:n two-seater:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sports car:1:n sport car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Stanley Steamer:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { stock car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { subcompact:1:n subcompact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { touring car:1:n phaeton:1:n tourer:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { used-car:1:n secondhand car:1:n }\n")    
           
  @Test def testSynsetsHoloPartsAndHoloPortionsCount() = result of ("count({}.partial_holonym|member_holonym)") should equal ("7\n")    

  @Test def testSynsetsHoloPartsAndHoloPortionsCountByAny() = result of ("count({}._[## = \\partial_holonym or ## = \\member_holonym])") should equal ("7\n")      
  
  @Test def testCarTransitiveHyponyms() = result of ("{car}.^hypernym!") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { ambulance:1:n } ^hypernym { funny wagon:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { beach wagon:1:n station wagon:1:n wagon:5:n estate car:1:n beach waggon:1:n station waggon:1:n waggon:2:n } ^hypernym { shooting brake:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { bus:4:n jalopy:1:n heap:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { gypsy cab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cab:3:n hack:5:n taxi:1:n taxicab:1:n } ^hypernym { minicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { compact:3:n compact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { convertible:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { coupe:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { cruiser:1:n police cruiser:1:n patrol car:1:n police car:1:n prowl car:1:n squad car:1:n } ^hypernym { panda car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { electric:1:n electric automobile:1:n electric car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { gas guzzler:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hardtop:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hatchback:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { horseless carriage:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { hot rod:1:n hot-rod:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { jeep:1:n landrover:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { limousine:1:n limo:1:n } ^hypernym { berlin:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { loaner:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minicar:1:n } ^hypernym { minicab:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { minivan:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Model T:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { pace car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n } ^hypernym { finisher:5:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { racer:2:n race car:1:n racing car:1:n } ^hypernym { stock car:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { roadster:1:n runabout:1:n two-seater:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sedan:1:n saloon:3:n } ^hypernym { brougham:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sports car:1:n sport car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { sport utility:1:n sport utility vehicle:1:n S.U.V.:1:n SUV:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { Stanley Steamer:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { stock car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { subcompact:1:n subcompact car:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { touring car:1:n phaeton:1:n tourer:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } ^hypernym { used-car:1:n secondhand car:1:n }\n")
    
  @Test def testCarTransitiveHypernyms() = result of ("{car:1}.hypernym!") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")

  @Test def testCarTransitiveHypernymsFiltered() = result of ("{car:1}.hypernym![# != {vehicle:1:n}]") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
  
  @Test def testNAryRelationSilentProjection() = result of ("car:1:n.literal") should equal ("car:1:n literal car\n")

  @Test def testNAryRelationIvertedProjection() = result of ("car.^literal") should equal ("car ^literal car:5:n\ncar ^literal car:1:n\ncar ^literal car:2:n\ncar ^literal car:4:n\ncar ^literal car:3:n\n")
  
  @Test def testNAryRelationIvertedProjectionFullDescription() = result of ("car.destination^literal^source") should equal ("car ^literal car:5:n\ncar ^literal car:1:n\ncar ^literal car:2:n\ncar ^literal car:4:n\ncar ^literal car:3:n\n")

  @Test def testNAryRelationPartialDestinationProjection() = result of ("car:1:n.literal^pos") should equal ("car:1:n source^literal^pos n\n")
  
  @Test def testNAryRelationPartialDestinationMultiProjection() = result of ("car:1:n.literal^pos^num") should equal ("car:1:n source^literal^pos n source^literal^num 1\n")

  @Test def testNAryRelationPartialSourceProjection() = result of ("count(`n`.pos^literal)") should equal ("175\n")
  
  @Test def testNAryRelationMultiProjection() = result of ("count(`n`.pos^literal^source^num)") should equal ("175\n")
  
  @Test def testNAryRelationInvalidProjection() = result of ("car:1:n.destination^num") should equal ("ERROR: Arc expression destination^num references an unknown relation or argument")
  
  @Test def testRegularPathFrom2To2() = result of ("{car}.hypernym{2}") should equal ("{ cable car:1:n car:5:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n }\n{ car:4:n elevator car:1:n } hypernym { compartment:2:n } hypernym { room:1:n }\n{ car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n")
  
  @Test def testRegularPathFrom2To3() = result of ("{car:1}.hypernym{2, 3}") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n }\n")

  @Test def testRegularPathFrom10ToEnd() = result of ("{car:1}.hypernym{10, }") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n } hypernym { conveyance:3:n transport:1:n } hypernym { instrumentality:3:n instrumentation:1:n } hypernym { artifact:1:n artefact:1:n } hypernym { whole:2:n unit:6:n } hypernym { object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")

  @Test def testRegularPathFrom0To2() = result of ("{car:1}.hypernym{, 2}") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n }\n")

  @Test def testRegularPathZeroOrMore() = result of ("{object:1}.hypernym*") should equal ("{ object:1:n physical object:1:n }\n{ object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
  
  @Test def testRegularPathOneOrMore() = result of ("{object:1}.hypernym+") should equal ("{ object:1:n physical object:1:n } hypernym { physical entity:1:n }\n{ object:1:n physical object:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
    
  @Test def testGenerateRegularPathFrom2To2() = result of ("size(hypernym{1})") should equal ("2\n")
  
  @Test def testGenerateRegularPathFrom2To3() = result of ("size(hypernym{2, 3})") should equal ("3\n4\n")

  @Test def testGenerateRegularPathFrom10ToEnd() = result of ("size(hypernym{10, })") should equal ("11\n12\n13\n14\n")

  @Test def testGenerateRegularPathFrom0To2() = result of ("size(hypernym{, 2})") should equal ("1\n2\n3\n")

  @Test def testGenerateRegularPathZeroOrMore() = result of ("size(hypernym*)") should equal ("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n")
  
  @Test def testGenerateRegularPathOneOrMore() = result of ("size(hypernym+)") should equal ("2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n")

  @Test def testSourceTypeSpecifier() = result of ("({car:1:n} union car).source&string^senses") should equal ("car senses car:5:n\ncar senses car:1:n\ncar senses car:2:n\ncar senses car:4:n\ncar senses car:3:n\n")

  @Test def testWrongSourceTypeSpecifier() = result of ("({car:1:n} union car).source&zzz^senses") should startWith ("ERROR: Datatype 'zzz' not found")

  @Test def testDestinationTypeSpecifier0() = result of ("car:1:n.destination^senses^source") should equal ("car:1:n ^senses { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testDestinationTypeSpecifier1() = result of ("car:1:n.destination^senses^source&synset") should equal ("car:1:n ^senses { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")

  @Test def testDestinationTypeSpecifier2() = result of ("car:1:n.destination^senses^source&string") should equal ("car:1:n ^senses car\n")

  @Test def testWrongDestinationTypeSpecifier0() = result of ("car:1:n.destination^senses^source&zzz") should startWith ("ERROR: Datatype 'zzz' not found")
}
