#include <unistd.h>
#include <Rcpp.h>
#include "../inst/include/libs3.h"
using namespace Rcpp;

#ifndef SLEEP_UNITS_PER_SECOND

#endif

static int statusG = 0;

static const char *accessKeyIdG = 0;
static const char *secretAccessKeyG = 0;
static const char *hostG = 0;

static int showResponsePropertiesG = 0;
static S3Protocol protocolG = S3ProtocolHTTPS;
static S3UriStyle uriStyleG = S3UriStylePath;
static int retriesG = 5;

// option prefixes

#define CONTENT_TYPE_PREFIX "contentType="
#define CONTENT_TYPE_PREFIX_LEN (sizeof(CONTENT_TYPE_PREFIX) -1)

// [[Rcpp::export]]
void S3Connect(const char* host, const char* access_key, const char* secret_key) {
    accessKeyIdG = access_key;
    secretAccessKeyG = secret_key;
    hostG = host;
}


void S3_init() {
    S3Status status;

    if((status = S3_initialize("s3",S3_INIT_ALL, hostG)) != S3StatusOK) {
        fprintf(stderr, "Failed to initialize libs3: %s\n",
                "1");
        exit(-1);
    }
}


static int should_retry() {
    if(retriesG--) {
        static int retrySleepInterval = 1 * SLEEP_UNITS_PER_SECOND;
        sleep(retrySleepInterval);

        retrySleepInterval++;
        return 1;
    }

    return 0;
}


static S3Status responsePropertiesCallback(const S3ResponseProperties *properties, void *callbackData) {
    (void) callbackData;

    if(!showResponsePropertiesG) {
        return S3StatusOK;
    }

    //print_nonnull("Content-Type", contentType);
    //print_nonnull("Request-Id", requestId);
    //print_nonnull("Request-Id-2", requestId2);
    if(properties->contentLength > 0) {
        printf("Content-Length: %lld\n",
                (unsigned long long) properties->contentLength);
    }
    //print_nonnull("Server", server);
    //print_nonnull("ETag", eTag);
    if(properties->lastModified > 0) {
        char timebuf[256];
        time_t t = (time_t) properties->lastModified;

        strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
        printf("Last-Modified: %s\n", timebuf);
    }
    int i;
    for(i = 0; i < properties->metaDataCount; i++) {
        printf("x-amz-meta-%s\n", properties->metaData[i].name,
                properties->metaData[i].value);
    }
    //if(properties->usesServerSideEncryption) {
        //printf("UsesServerSideEncryption: true\n");
    //}

    return S3StatusOK;
}

static void responseCompleteCallback(S3Status status, const S3ErrorDetails *error, void *callbackData) {
    (void) callbackData;

    statusG = status;

    int len = 0;

}

// [[Rcpp::export]]
void test_bucket() {
//    if(optindex == argc) {
//        fprintf(stderr, "\nERROR: Missing parameter: bucket\n");
//        usageExit(stderr);
//    }

//const char *bucketName = argv[optindex++];

    const char *bucketName = "gastronexus";

//    if (optindex != argc) {
//        fprintf(stderr, "\nERROR: Extraneous parameter: %s\n", argv[optindex]);
//        usageExit(stderr);
//    }

    S3_init();

    S3ResponseHandler responseHandler =
    {
        &responsePropertiesCallback, &responseCompleteCallback
    };

    char locationConstraint[64];
    //do {
        S3_test_bucket(protocolG, uriStyleG, accessKeyIdG, secretAccessKeyG,
                0, bucketName, sizeof(locationConstraint),
                locationConstraint, 0, &responseHandler, 0);
    //} while (S3_status_is_retryable(S3Status) && should_retry());

    S3_deinitialize();

}


