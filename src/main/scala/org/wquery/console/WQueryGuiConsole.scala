package org.wquery.console
import javax.swing.UIManager
import javax.swing.filechooser.FileNameExtensionFilter
import java.awt.Dimension
import java.io.{BufferedWriter, FileWriter, File}
import org.wquery.{WQueryProperties, WQuery}
import org.wquery.emitter.{XmlWQueryEmitter, PlainWQueryEmitter}
import scala.io.Source
import scala.swing.{SimpleSwingApplication, TextArea, Frame, FileChooser, MenuBar, Menu, MenuItem, Separator, Dialog, BoxPanel, Orientation, Swing, ScrollPane, Component, Button, TabbedPane}
import scala.swing.event.{ButtonClicked, WindowClosing, Event}

object WQueryGuiConsole extends SimpleSwingApplication {    
  var args: Array[String] = null
  val plainEmitter = new PlainWQueryEmitter
  val xmlEmitter = new XmlWQueryEmitter
  
  def top = createConsole
  
  def createConsole: Frame = new Frame {
    var wquery: WQuery = null
    var queryArea: TextArea = null
    var plainResultArea: TextArea = null
    var xmlResultArea: TextArea = null
    
    if (!args.isEmpty) {
        wquery = WQuery.createInstance(args.head)
        title = new File(args.head).getAbsolutePath + " - WQuery Console"
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
            wquery = WQuery.createInstance(file.getAbsolutePath)
            title = file.getAbsolutePath + " - WQuery Console"
          })
        }
        
        contents += new MenuItem("Load Queries...") {
          reactions += dialogApproved(this, queriesChooser, "Load", { file =>
            Source.fromFile(file).getLines.foreach(queryArea.text += _ + '\n')
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
      
      contents += Swing.RigidBox(new Dimension(5, 5))
      
      contents += new Button {
        text = "Execute"
        
        xLayoutAlignment = java.awt.Component.CENTER_ALIGNMENT // instead of horizontalAlignment = Alignment.Center
               
        peer.setMnemonic(java.awt.event.KeyEvent.VK_E)        
        
        reactions += {
          case ButtonClicked(_) =>
            if (wquery != null) {
              if (!queryArea.text.trim.isEmpty) {
                val result = wquery.execute(queryArea.text)
                plainResultArea.text = plainEmitter.emit(result)
                xmlResultArea.text = xmlEmitter.emit(result)
              }
            } else {               
              Dialog.showConfirmation(this, "You have to load a wordnet before executing a query (File -> Load Wordnet...)", 
                "Error", Dialog.Options.Default, Dialog.Message.Error, null)
            }
        }
      }
      
      contents += Swing.RigidBox(new Dimension(5, 5))
      
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
        System.exit(0)    
    }  
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
