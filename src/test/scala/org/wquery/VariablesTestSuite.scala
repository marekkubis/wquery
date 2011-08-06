package org.wquery
import org.testng.annotations.Test

class VariablesTestSuite extends WQueryTestSuite {
  @Test def generatorWithStepVariable() = result of ("{car}$a") should equal ("$a={ cable car:1:n car:5:n } { cable car:1:n car:5:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n$a={ car:4:n elevator car:1:n } { car:4:n elevator car:1:n }\n$a={ car:3:n gondola:3:n } { car:3:n gondola:3:n }\n")

  @Test def generatorWithPathVariable() = result of ("{car}@P") should equal ("@P=({ cable car:1:n car:5:n }) { cable car:1:n car:5:n }\n@P=({ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }) { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n@P=({ car:2:n railcar:1:n railway car:1:n railroad car:1:n }) { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n@P=({ car:4:n elevator car:1:n }) { car:4:n elevator car:1:n }\n@P=({ car:3:n gondola:3:n }) { car:3:n gondola:3:n }\n")  
  
  @Test def generatorWithTooManyStepVariables() = result of ("{car}$a$b") should startWith ("ERROR: Variable $a cannot be bound")

  @Test def generatorWithTooManyPathVariables() = result of ("{car}@P@Q") should startWith ("ERROR: Variable list @P@Q contains more than one path variable")
  
  @Test def generatorWithTooManyVariables() = result of ("{car}$a$P") should startWith ("ERROR: Variable $a cannot be bound")  
  
  @Test def stepWithOneStepVariable() = result of ("{car}.hypernym$a") should equal ("$a={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def stepWithOnePathVariable() = result of ("{car}.hypernym@P") should equal ("@P=({ cable car:1:n car:5:n } hypernym { compartment:2:n }) { cable car:1:n car:5:n } hypernym { compartment:2:n }\n@P=({ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }) { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n@P=({ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }) { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n@P=({ car:4:n elevator car:1:n } hypernym { compartment:2:n }) { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n@P=({ car:3:n gondola:3:n } hypernym { compartment:2:n }) { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")  
  
  @Test def stepWithTwoStepVariables() = result of ("{car}.hypernym$a$b") should equal ("$a=hypernym $b={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a=hypernym $b={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a=hypernym $b={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a=hypernym $b={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a=hypernym $b={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def stepWithStepAndPathVariable() = result of ("{car}.hypernym$a@P") should equal ("@P=(hypernym { compartment:2:n }) $a={ cable car:1:n car:5:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n@P=(hypernym { motor vehicle:1:n automotive vehicle:1:n }) $a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n@P=(hypernym { wheeled vehicle:1:n }) $a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n@P=(hypernym { compartment:2:n }) $a={ car:4:n elevator car:1:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n@P=(hypernym { compartment:2:n }) $a={ car:3:n gondola:3:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def stepWithPathAndStepVariable() = result of ("{car}.hypernym@P$a") should equal ("@P=({ cable car:1:n car:5:n } hypernym) $a={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n@P=({ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym) $a={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n@P=({ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym) $a={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n@P=({ car:4:n elevator car:1:n } hypernym) $a={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n@P=({ car:3:n gondola:3:n } hypernym) $a={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")  
  
  @Test def stepWithThreeStepVariables() = result of ("{car}.hypernym$a$b$c") should equal ("$a={ cable car:1:n car:5:n } $b=hypernym $c={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b=hypernym $c={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $b=hypernym $c={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ car:4:n elevator car:1:n } $b=hypernym $c={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ car:3:n gondola:3:n } $b=hypernym $c={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def stepWithThreeVariables() = result of ("{car}.hypernym$a@P$c") should equal ("@P=(hypernym) $a={ cable car:1:n car:5:n } $c={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n@P=(hypernym) $a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $c={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n@P=(hypernym) $a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $c={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n@P=(hypernym) $a={ car:4:n elevator car:1:n } $c={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n@P=(hypernym) $a={ car:3:n gondola:3:n } $c={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")  
  
  @Test def stepWithVariablePlaceHolder() = result of ("{car}.hypernym$a$_$c") should equal ("$a={ cable car:1:n car:5:n } $c={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $c={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $c={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ car:4:n elevator car:1:n } $c={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ car:3:n gondola:3:n } $c={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def stepWithPathVariablePlaceHolder() = result of ("{car}.hypernym$a@_") should equal ("$a={ cable car:1:n car:5:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ car:4:n elevator car:1:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ car:3:n gondola:3:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")
    
  @Test def twoStepsWithStepVariables() = result of ("{car}$a.hypernym$b") should equal ("$a={ cable car:1:n car:5:n } $b={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $b={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ car:4:n elevator car:1:n } $b={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ car:3:n gondola:3:n } $b={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")
  
  @Test def twoStepsWithPathVariables() = result of ("{car}@P.hypernym@Q") should equal ("@P=({ cable car:1:n car:5:n }) @Q=({ cable car:1:n car:5:n } hypernym { compartment:2:n }) { cable car:1:n car:5:n } hypernym { compartment:2:n }\n@P=({ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }) @Q=({ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }) { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n@P=({ car:2:n railcar:1:n railway car:1:n railroad car:1:n }) @Q=({ car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }) { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n@P=({ car:4:n elevator car:1:n }) @Q=({ car:4:n elevator car:1:n } hypernym { compartment:2:n }) { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n@P=({ car:3:n gondola:3:n }) @Q=({ car:3:n gondola:3:n } hypernym { compartment:2:n }) { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")  
  
  @Test def threeStepsWithVariables() = result of ("{car}$a.hypernym$b.hypernym$c") should equal ("$a={ cable car:1:n car:5:n } $b={ compartment:2:n } $c={ room:1:n } { cable car:1:n car:5:n } hypernym { compartment:2:n } hypernym { room:1:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b={ motor vehicle:1:n automotive vehicle:1:n } $c={ self-propelled vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n } hypernym { self-propelled vehicle:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $b={ wheeled vehicle:1:n } $c={ container:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n } hypernym { container:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $b={ wheeled vehicle:1:n } $c={ vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n } hypernym { vehicle:1:n }\n$a={ car:4:n elevator car:1:n } $b={ compartment:2:n } $c={ room:1:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n } hypernym { room:1:n }\n$a={ car:3:n gondola:3:n } $b={ compartment:2:n } $c={ room:1:n } { car:3:n gondola:3:n } hypernym { compartment:2:n } hypernym { room:1:n }\n")
  
  @Test def unionRelationalExprWithStepVariables() = result of ("{room}$a.partial_holonym|hypernym$b") should equal ("$a={ room:1:n } $b={ area:5:n } { room:1:n } hypernym { area:5:n }\n$a={ room:1:n } $b={ building:1:n edifice:1:n } { room:1:n } partial_holonym { building:1:n edifice:1:n }\n")

  @Test def unionRelationalExprWithPathVariables() = result of ("{room}@P.partial_holonym|hypernym@Q") should equal ("@P=({ room:1:n }) @Q=({ room:1:n } hypernym { area:5:n }) { room:1:n } hypernym { area:5:n }\n@P=({ room:1:n }) @Q=({ room:1:n } partial_holonym { building:1:n edifice:1:n }) { room:1:n } partial_holonym { building:1:n edifice:1:n }\n")

  @Test def quantifiedRelationalExprWithStepVariables() = result of ("{person:1:n}$a.hypernym!$b") should equal ("$a={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } $b={ organism:1:n being:2:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n$a={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } $b={ causal agent:1:n cause:4:n causal agency:1:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n$a={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } $b={ physical entity:1:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n } hypernym { physical entity:1:n }\n$a={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } $b={ entity:1:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")

  @Test def quantifiedRelationalExprWithPathVariables() = result of ("{person:1:n}@P.hypernym!@Q") should equal ("@P=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }) @Q=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }) { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { organism:1:n being:2:n }\n@P=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }) @Q=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }) { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n }\n@P=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }) @Q=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n } hypernym { physical entity:1:n }) { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n } hypernym { physical entity:1:n }\n@P=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }) @Q=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }) { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } hypernym { causal agent:1:n cause:4:n causal agency:1:n } hypernym { physical entity:1:n } hypernym { entity:1:n }\n")
  
  @Test def crossProductWithStepVariables() = result of ("{apple}$a,{person}$b") should equal ("$a={ apple:1:n } $b={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { apple:1:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n$a={ apple:1:n } $b={ person:2:n } { apple:1:n } { person:2:n }\n$a={ apple:1:n } $b={ person:3:n } { apple:1:n } { person:3:n }\n$a={ apple:2:n orchard apple tree:1:n Malus pumila:1:n } $b={ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n } { apple:2:n orchard apple tree:1:n Malus pumila:1:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n$a={ apple:2:n orchard apple tree:1:n Malus pumila:1:n } $b={ person:2:n } { apple:2:n orchard apple tree:1:n Malus pumila:1:n } { person:2:n }\n$a={ apple:2:n orchard apple tree:1:n Malus pumila:1:n } $b={ person:3:n } { apple:2:n orchard apple tree:1:n Malus pumila:1:n } { person:3:n }\n")

  @Test def crossProductWithPathVariables() = result of ("{apple}@P,{person}@Q") should equal ("@P=({ apple:1:n }) @Q=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }) { apple:1:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n@P=({ apple:1:n }) @Q=({ person:2:n }) { apple:1:n } { person:2:n }\n@P=({ apple:1:n }) @Q=({ person:3:n }) { apple:1:n } { person:3:n }\n@P=({ apple:2:n orchard apple tree:1:n Malus pumila:1:n }) @Q=({ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }) { apple:2:n orchard apple tree:1:n Malus pumila:1:n } { person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n@P=({ apple:2:n orchard apple tree:1:n Malus pumila:1:n }) @Q=({ person:2:n }) { apple:2:n orchard apple tree:1:n Malus pumila:1:n } { person:2:n }\n@P=({ apple:2:n orchard apple tree:1:n Malus pumila:1:n }) @Q=({ person:3:n }) { apple:2:n orchard apple tree:1:n Malus pumila:1:n } { person:3:n }\n")  
  
  @Test def filterWithVariable() = result of ("{car}$a[$a = {car:1}]") should equal ("$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
    
  @Test def filterWithAnonymousVariable() = result of ("{car}$_a[$_a = {car:1}]") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")  
    
  @Test def filterWithVariableAfterSecondStep() = result of ("last({person:1}.senses$a[$a != individual:1:n])") should equal ("person:1:n\nsomeone:1:n\nsomebody:1:n\nmortal:1:n\nsoul:2:n\n")
     
  @Test def variablesInTwoFilters() = result of ("last({person}.senses$a[$a != person:2:n].word$b[$b != mortal])") should equal ("individual\nperson\nsomebody\nsomeone\nsoul\n")    
    
  @Test def twoVariablesInFilter() = result of ("last({person}.senses$a.word$b[$a != individual:1:n and $b != mortal])") should equal ("person\nsomebody\nsomeone\nsoul\n")

  @Test def stepAfterAnonymousVariable() = result of ("{person}$_a[{organism:1:n} in $_a.hypernym]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")     

  @Test def anonymousVariableAsFunctionParam() = result of ("{person}$_a[count($_a.hypernym)>1]") should equal ("{ person:1:n individual:1:n someone:1:n somebody:1:n mortal:1:n soul:2:n }\n")
  
  @Test def variableAfterFilter() = result of ("{car}[# = {car:1}]$a") should equal ("$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n")
  
  @Test def overlappingStepVariables() = result of ("{car}.hypernym$a$b@P$c$d") should equal ("@P=() $a={ cable car:1:n car:5:n } $b=hypernym $c=hypernym $d={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n@P=() $a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b=hypernym $c=hypernym $d={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n@P=() $a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $b=hypernym $c=hypernym $d={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n@P=() $a={ car:4:n elevator car:1:n } $b=hypernym $c=hypernym $d={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n@P=() $a={ car:3:n gondola:3:n } $b=hypernym $c=hypernym $d={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def emptyPathVariable() = result of ("{car}.hypernym$a$b@P$c$d$e") should equal ("@P=() $a={ cable car:1:n car:5:n } $b=hypernym $c={ cable car:1:n car:5:n } $d=hypernym $e={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n@P=() $a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b=hypernym $c={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $d=hypernym $e={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n@P=() $a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $b=hypernym $c={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $d=hypernym $e={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n@P=() $a={ car:4:n elevator car:1:n } $b=hypernym $c={ car:4:n elevator car:1:n } $d=hypernym $e={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n@P=() $a={ car:3:n gondola:3:n } $b=hypernym $c={ car:3:n gondola:3:n } $d=hypernym $e={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")  
  
  @Test def tooManyLeftSideStepVariables() = result of ("{car}.hypernym$a$b$x$y@P$c$d") should startWith ("ERROR: Variable $y cannot be bound")
  
  @Test def tooManyRightSideStepVariables() = result of ("{car}.hypernym$a$b@P$c$d$x$y") should startWith ("ERROR: Variable $c cannot be bound")  

  @Test def tooManyStepVariables() = result of ("{car}.hypernym$a$b$c$d@P$w$x$y$z") should startWith ("ERROR: Variable $w cannot be bound")
  
  @Test def duplicatedVariables() = result of ("{car}.hypernym$a$a") should startWith ("ERROR: Variable list $a$a contains duplicated variable names")
      
  @Test def stepVariablesInProjection() = result of ("{car}.hypernym$a$_$b<$b,$a>") should equal ("{ compartment:2:n } { cable car:1:n car:5:n }\n{ compartment:2:n } { car:4:n elevator car:1:n }\n{ compartment:2:n } { car:3:n gondola:3:n }\n{ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n")
  
  @Test def mixedVariablesInProjection() = result of ("{car}.hypernym$a@P<@P,$a>") should equal ("hypernym { compartment:2:n } { cable car:1:n car:5:n }\nhypernym { compartment:2:n } { car:4:n elevator car:1:n }\nhypernym { compartment:2:n } { car:3:n gondola:3:n }\nhypernym { motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\nhypernym { wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n")
  
  @Test def duplicatedVariableInProjection() = result of ("{car}.hypernym$a<$a,$a>") should equal ("{ compartment:2:n } { compartment:2:n }\n{ motor vehicle:1:n automotive vehicle:1:n } { motor vehicle:1:n automotive vehicle:1:n }\n{ wheeled vehicle:1:n } { wheeled vehicle:1:n }\n")
  
  @Test def unboundVariableInProjection() = result of ("{car}.hypernym$a<$z>") should startWith ("ERROR: A reference to unknown variable $z found")
      
  @Test def twoStepsWithStepVariablesProjection() = result of ("{car}$a.hypernym$b<$a,$b>") should equal ("{ cable car:1:n car:5:n } { compartment:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { wheeled vehicle:1:n }\n{ car:4:n elevator car:1:n } { compartment:2:n }\n{ car:3:n gondola:3:n } { compartment:2:n }\n")
  
  @Test def twoStepsWithPathVariablesProjection() = result of ("{car}@P.hypernym@Q<@P,@Q>") should equal ("{ cable car:1:n car:5:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n{ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n{ car:4:n elevator car:1:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n{ car:3:n gondola:3:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")  
    
  @Test def multipleVariableInMultipleStepsProjection() = result of ("{car}$a.hypernym$b$c<$c,$b,$a>") should equal ("{ compartment:2:n } hypernym { cable car:1:n car:5:n }\n{ compartment:2:n } hypernym { car:4:n elevator car:1:n }\n{ compartment:2:n } hypernym { car:3:n gondola:3:n }\n{ motor vehicle:1:n automotive vehicle:1:n } hypernym { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n{ wheeled vehicle:1:n } hypernym { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n")  

  @Test def stepVariablePrecedingPathVariableUsedInFiler() = result of ("{car}.hypernym$_a@_[$_a = {car:1:n}]") should equal ("{ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n")

  @Test def variableReferenceInNestedFiler() = result of ("{car}$a[count(hypernym[$a != {car:1:n}]) > 0]") should equal ("$a={ cable car:1:n car:5:n } { cable car:1:n car:5:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n$a={ car:4:n elevator car:1:n } { car:4:n elevator car:1:n }\n$a={ car:3:n gondola:3:n } { car:3:n gondola:3:n }\n")

}