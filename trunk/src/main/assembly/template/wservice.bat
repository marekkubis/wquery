rem
rem	wservice.bat
rem		WQuery web service
rem

java %JAVA_OPTS% -classpath "${project.build.finalName}.${project.packaging}" org.wquery.service.WQueryService %*
