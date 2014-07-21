#include <stdio.h>
#include <unistd.h>
#include <RcppArmadillo.h>
#include "../inst/include/libs3.h"
using namespace Rcpp;

#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif


static const char *accessKeyIdG = 0;
static const char *secretAccessKeyG = 0;
static const char *hostG = 0;

static int forceG = 0;
static int showResponsePropertiesG = 0;
static S3Protocol protocolG = S3ProtocolHTTPS;
static S3UriStyle uriStyleG = S3UriStylePath;
static int retriesG = 5;

static S3Status statusG;
static char errorDetailsG[4096] = { 0 };

// option prefixes

#define CONTENT_TYPE_PREFIX "contentType="
#define CONTENT_TYPE_PREFIX_LEN (sizeof(CONTENT_TYPE_PREFIX) -1)

// [[Rcpp::export]]
void S3Connect(const char* access_key, const char* secret_key) {
    accessKeyIdG = access_key;
    secretAccessKeyG = secret_key;
}


int S3_init() {
    S3Status status;

    if((status = S3_initialize("s3",S3_INIT_ALL, hostG)) != S3StatusOK) {
        fprintf(stderr, "Failed to initialize libs3: %s\n",
                "1");
        Rcout << "\nFailed to initialize\n";
        return 0;
    }
    Rcout << "\ninitialized correctly\n";
}

static void printError() {
    if(statusG < S3StatusErrorAccessDenied) {
        Rcout << S3_get_status_name(statusG);
    }
    else {
        Rcout << S3_get_status_name(statusG);
        Rcout << errorDetailsG;
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
#define print_nonnull(name, field) \
    do { \
        if(properties-> field) { \
            Rcout << "name = " << name << ", properties = " << properties->field; \
        } \
    } while(0) 

    print_nonnull("Content-Type", contentType);
    print_nonnull("Request-Id", requestId);
    print_nonnull("Request-Id-2", requestId2);
    if(properties->contentLength > 0) {
        printf("Content-Length: %lld\n",
                (unsigned long long) properties->contentLength);
    }
    print_nonnull("Server", server);
    print_nonnull("ETag", eTag);
    if(properties->lastModified > 0) {
        char timebuf[256];
        time_t t = (time_t) properties->lastModified;

        strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
        Rcout << printf("Last-Modified: %s\n", timebuf);
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
void test_bucket(const char* bucketName) {


    S3_init();

    S3ResponseHandler responseHandler =
    {
        &responsePropertiesCallback, &responseCompleteCallback
    };

    char locationConstraint[64];
    do {
        S3_test_bucket(protocolG, uriStyleG, accessKeyIdG, secretAccessKeyG,
                0, bucketName, sizeof(locationConstraint),
                locationConstraint, 0, &responseHandler, 0);
    } while (S3_status_is_retryable(statusG) && should_retry());

    const char *result;

    switch (statusG) {
        case S3StatusOK:
            // bucket exists
            result = locationConstraint[0] ? locationConstraint : "USA";
            break;
        case S3StatusErrorNoSuchBucket:
            result = "Does Not Exist";
            break;
        case S3StatusErrorAccessDenied:
            result = "Access Denied";
            break;
        default:
            result = 0;
            break;
    }

    if(result) {
        Rcout << result;
    }
    else {
        printError();
    }

//(*(responseHandler.propertiesCallback))(NULL,NULL);
    S3_deinitialize();


}

// [[Rcpp::export]]
int create_bucket(const char* bucketName, const char* acl = "private") {

    if (!forceG && (S3_validate_bucket_name
                (bucketName, S3UriStyleVirtualHost) != S3StatusOK)) {
        Rcout << "\nWARNING: Bucket name is not valid for virtual-host style URI access.\n";
        Rcout << "\nBucket not created. Use 'force=TRUE' option to force the bucket to be created despite this warning.\n";
        return 0;
    }

    const char *locationConstraint = 0;
    S3CannedAcl cannedAcl = S3CannedAclPrivate;
    if(!strcmp(acl,"private")) {
        cannedAcl = S3CannedAclPrivate;
    }
    else if(!strcmp(acl, "public-read")) {
        cannedAcl = S3CannedAclPublicRead;
    }
    else if(!strcmp(acl, "public-read-write")) {
        cannedAcl = S3CannedAclPublicReadWrite;
    }
    else if(!strcmp(acl, "authenticated-read")) {
        cannedAcl = S3CannedAclAuthenticatedRead;
    }
    else {
        Rcout << "\nError: Unknown canned ACL: %s\n" << acl;
        return 0;
    }

    S3_init();

    S3ResponseHandler responseHandler =
    {
        &responsePropertiesCallback, &responseCompleteCallback
    };

   do {
        S3_create_bucket(protocolG, accessKeyIdG, secretAccessKeyG, 0, bucketName, cannedAcl,
                locationConstraint, 0, &responseHandler, 0);
    } while(S3_status_is_retryable(statusG) && should_retry());

    if(statusG == S3StatusOK) {
        Rcout << "Bucket successfully created.\n";
    }
    else {
        printError();
    }

   //
    S3_deinitialize();
}

// [[Rcpp::export]]
void delete_bucket(const char* bucketName) {

    S3_init();

    S3ResponseHandler responseHandler =
    {
        &responsePropertiesCallback, &responseCompleteCallback
    };

    do {
        S3_delete_bucket(protocolG, uriStyleG, accessKeyIdG, secretAccessKeyG,
                0, bucketName, 0, &responseHandler, 0);
    } while(S3_status_is_retryable(statusG) && should_retry());

    if(statusG != S3StatusOK) {
        printError();
    }

    S3_deinitialize();
}

