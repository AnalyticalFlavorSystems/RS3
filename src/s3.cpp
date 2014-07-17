#include <stdint.h>
#include <sys/select.h>
#include <ctype.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <Rcpp.h>
#include <libs3.h>
using namespace Rcpp;


// [[Rcpp::export]]
void S3_init(const char* host) {
    S3Status status;
    S3_initialize("s3",S3_INIT_ALL, host);

    if((status = S3_initialize("s3",S3_INIT_ALL, host)) != S3StatusOK) {
        fprintf(stderr, "Failed to initialize libs3: %s\n",
                "404");
        exit(-1);
    }
}
