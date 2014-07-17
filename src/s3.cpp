#include <Rcpp.h>
#include "../inst/include/libs3.h"
using namespace Rcpp;

// [[Rcpp::export]]
void S3Connect(const char* access_key, const char* secret_key) {
    accessKeyIdG = access_key;
    secretAccessKeyG = secret_key;
}
    


// [[Rcpp::export]]
void S3_init(const char* host) {
    S3Status status;

    if((status = S3_initialize("s3",S3_INIT_ALL, host)) != S3StatusOK) {
        fprintf(stderr, "Failed to initialize libs3: %s\n",
                "1");
        exit(-1);
    }
}

//// [[Rcpp::export]]
//void test_bucket(const char* host, int argc, int optindex, char *argv[]) {
//    if(optindex == argc) {
//        fprintf(stderr, "\nERROR: Missing parameter: bucket\n");
//        usageExit(stderr);
//    }
//
//    const char *bucketName = argv[optindex++];
//
//    if (optindex != argc) {
//        fprintf(stderr, "\nERROR: Extraneous parameter: %s\n", argv[optindex]);
//        usageExit(stderr);
//    }
//
//    S3_init(host);
//
//
//}


