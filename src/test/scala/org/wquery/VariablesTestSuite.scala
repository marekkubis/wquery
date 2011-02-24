package org.wquery
import org.testng.annotations.Test

class VariablesTestSuite extends WQueryTestSuite {
  @Test def testGeneratorWithStepVariable() = result of ("{car}$a") should equal ("$a={ cable car:1:n car:5:n } { cable car:1:n car:5:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n }\n$a={ car:4:n elevator car:1:n } { car:4:n elevator car:1:n }\n$a={ car:3:n gondola:3:n } { car:3:n gondola:3:n }\n")

  @Test def generatorWithTooManyStepVariables() = result of ("{car}$a$b") should startWith ("ERROR: Variable $a cannot be bound")

  @Test def stepWithOneVariables() = result of ("{car}.hypernym$a") should equal ("$a={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")
  
  @Test def stepWithTwoVariables() = result of ("{car}.hypernym$a$b") should equal ("$a=hypernym $b={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a=hypernym $b={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a=hypernym $b={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a=hypernym $b={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a=hypernym $b={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def stepWithThreeVariables() = result of ("{car}.hypernym$a$b$c") should equal ("$a={ cable car:1:n car:5:n } $b=hypernym $c={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $b=hypernym $c={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $b=hypernym $c={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ car:4:n elevator car:1:n } $b=hypernym $c={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ car:3:n gondola:3:n } $b=hypernym $c={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")

  @Test def stepWithUnnamedVariable() = result of ("{car}.hypernym$a$_$c") should equal ("$a={ cable car:1:n car:5:n } $c={ compartment:2:n } { cable car:1:n car:5:n } hypernym { compartment:2:n }\n$a={ car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } $c={ motor vehicle:1:n automotive vehicle:1:n } { car:1:n auto:1:n automobile:1:n machine:6:n motorcar:1:n } hypernym { motor vehicle:1:n automotive vehicle:1:n }\n$a={ car:2:n railcar:1:n railway car:1:n railroad car:1:n } $c={ wheeled vehicle:1:n } { car:2:n railcar:1:n railway car:1:n railroad car:1:n } hypernym { wheeled vehicle:1:n }\n$a={ car:4:n elevator car:1:n } $c={ compartment:2:n } { car:4:n elevator car:1:n } hypernym { compartment:2:n }\n$a={ car:3:n gondola:3:n } $c={ compartment:2:n } { car:3:n gondola:3:n } hypernym { compartment:2:n }\n")
    
}


