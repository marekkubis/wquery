package org.wquery.console

import scala.io.Source
import scala.swing._
import scala.swing.Swing._
import scala.swing.event._

import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import javax.swing.UIManager
import javax.swing.filechooser.FileNameExtensionFilter

import org.wquery.emitter.PlainLineWQueryEmitter
import org.wquery.emitter.XmlWQueryEmitter

object WQueryGuiConsole extends SimpleGUIApplication {    
  var args: Array[String] = null
  var frameCount = 0 
  val plainEmitter = new PlainLineWQueryEmitter
  val xmlEmitter = new XmlWQueryEmitter
  
  def top = createConsole
  
  def createConsole: Frame = new Frame {
    var wquery: WQuery = null
    var queryArea: TextArea = null
    var plainResultArea: TextArea = null
    var xmlResultArea: TextArea = null
    
    if (!args.isEmpty && frameCount == 1) {
        wquery = WQuery.getInstance(args.first)
        title = new File(args.first).getAbsolutePath + " - WQuery Console"
    } else {
        title = "WQuery Console"
    }    

    val wordNetChooser = new FileChooser(new File("."))    
    wordNetChooser.fileFilter = new FileNameExtensionFilter("Global WordNet Grid XML file", "xml")

    val queriesChooser = new FileChooser(new File("."))    
    queriesChooser.fileFilter = new FileNameExtensionFilter("WQuery queries file", "wq")

    val resultChooser = new FileChooser(new File("."))    
    resultChooser.peer.addChoosableFileFilter(new FileNameExtensionFilter("Plain text result", "txt"))
    resultChooser.peer.addChoosableFileFilter(new FileNameExtensionFilter("XML result", "xml"))
    
    menuBar = new MenuBar {
      contents += new Menu("File") {
        contents += new MenuItem("New Console...") {
          reactions += {
            case ButtonClicked(_) => 
              createConsole.visible = true              
          }
        }
        
        contents += new Separator
        
        contents += new MenuItem("Load Wordnet...") {
          reactions += dialogApproved(this, wordNetChooser, "Load", { file =>
            wquery = WQuery.getInstance(file.getAbsolutePath)     
            title = file.getAbsolutePath + " - WQuery Console"
          })
        }
        
        contents += new MenuItem("Load Queries...") {
          reactions += dialogApproved(this, queriesChooser, "Load", { file =>
            for (line <- Source.fromFile(file).getLines) {
              queryArea.text += line
            }            
          })          
        }
        
        contents += new MenuItem("Save Queries As...") {          
          reactions += dialogApproved(this, queriesChooser, "Save", { file =>
            val fileSuffix = if (file.getName.indexOf(".") == -1 && 
                                   queriesChooser.fileFilter.getDescription.startsWith("WQuery")) ".wq" else ""
            
            val writer = new BufferedWriter(new FileWriter(file.getAbsolutePath + fileSuffix))
            writer.write(queryArea.text)
            writer.close            
          })
        }
        
        contents += new MenuItem("Save Result As...") {
          reactions += dialogApproved(this, resultChooser, "Save", { file =>
            val fileSuffix = if (file.getName.indexOf(".") == -1) {
              if (resultChooser.fileFilter.getDescription.startsWith("XML")) ".xml" else ".txt"             
            } else  {
              ""
            }
            
            val area = if (fileSuffix == ".xml") xmlResultArea else plainResultArea            
            val writer = new BufferedWriter(new FileWriter(file.getAbsolutePath + fileSuffix))
            writer.write(area.text)
            writer.close            
          })
        }
        
        contents += new Separator
        
        contents += new MenuItem("Exit") {
          reactions += {
            case ButtonClicked(_) => 
              System.exit(0)                
          }                    
        }       
      } 
      
//      contents += new Menu("Edit") {
//        contents += new MenuItem("Undo") 
//        contents += new MenuItem("Redo")        
//      }
                                   
      contents += new Menu("Help") {
        contents += new MenuItem("About WQuery") {
          reactions += {
            case ButtonClicked(_) => 
              Dialog.showConfirmation(this, 
                                      "WQuery (" + WQueryProperties.version + 
                                        ") GUI Console\n" + WQueryProperties.copyright, 
                                      "About", Dialog.Options.Default, Dialog.Message.Info, null)
          }
        }        
      }      
    }        

    contents = new BoxPanel(Orientation.Vertical) {     
      border = Swing.EmptyBorder(5, 5, 5, 5)
      
      queryArea = new TextArea {
        columns = 40
        rows = 8
      }
      
      contents += new ScrollPane(queryArea)
      
      contents += RigidBox((5, 5))
      
      contents += new Button {
        text = "Execute"
        
        xLayoutAlignment = java.awt.Component.CENTER_ALIGNMENT // instead of horizontalAlignment = Alignment.Center
               
        peer.setMnemonic(java.awt.event.KeyEvent.VK_E)        
        
        reactions += {
          case ButtonClicked(_) =>
            if (!queryArea.text.trim.isEmpty) {
              val result = wquery.execute(queryArea.text)
              plainResultArea.text = plainEmitter.emit(result)
              xmlResultArea.text = xmlEmitter.emit(result)
            }
        }
      }
      
      contents += RigidBox((5, 5))
      
      plainResultArea = new TextArea {
        columns = 40
        rows = 15        
      }
      
      xmlResultArea = new TextArea {
        columns = 40
        rows = 15        
      }
      
      contents += new TabbedPane {
        pages += new TabbedPane.Page("Plain", new ScrollPane(plainResultArea))
        pages += new TabbedPane.Page("XML", new ScrollPane(xmlResultArea))        
      }             
    }
    
    reactions += {
      case WindowClosing(_) =>
        frameCount -= 1
        if (frameCount <= 1) // ATTENTION: first frame seems to be instantiated twice
            System.exit(0)    
    }
    
    frameCount += 1        
  }  
  
  override def main(args: Array[String]) {
    try {
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName)
    } catch {
      case _ => 
        // do nothing
    } 
    
    this.args = args
    super.main(args)
  }
  
  private def dialogApproved(parent: Component, chooser: FileChooser,
                             label: String, handler: File => Unit): PartialFunction[Event, Unit] = {
    { 
      case ButtonClicked(_) => 
        chooser.showDialog(parent, label) match {
          case FileChooser.Result.Approve =>
            handler(chooser.selectedFile)
          case _ => 
            // do nothing
        }                
    } 
  }
}
