RS3 is a interface to S3 using libs3.  Using RS3 you can do many tasks including:

* Create/delete a bucket
* upload/download a file
* set-acl on bucket
* get/set-logging on bucket

## Installing Linux and MacOS ##
Make sure that `libxml2-dev` and `libcurl3-dev` is installed

In R, install devtools and run `install_github.com("RS3", "Gastrograph")`


## Installing in Windows ##
Currently it works partially for windows in 32-bit R.  
If you use Rstudio, make sure to change R to use the 32-bit version.

Install the dlls located in `libs3-libs\bin` into `C:\libs3` 
Add the directory to your path then run `install_github.com("RS3", "Gastrograph")`

