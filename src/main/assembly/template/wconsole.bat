rem
rem	wconsole.bat
rem		WQuery interactive console
rem

java %JAVA_OPTS% -classpath "${project.build.finalName}.${project.packaging}" org.wquery.console.WQueryGuiConsole %*
