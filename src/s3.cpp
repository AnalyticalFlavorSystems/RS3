#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <RcppArmadillo.h>
#include "../inst/include/libs3.h"
using namespace Rcpp;

// some windows stuff
#ifndef FOPEN_EXTRA_FLAGS
#define FOPEN_EXTRA_FLAGS ""
#endif

// some unix stuff (to work around windows issues)
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

static uint64_t convertInt(const char* str, const char* paramName) {
    uint64_t ret = 0;

    while(*str) {
        if(!isdigit(*str)) {
            Rcout << "\nERROR: Nondigit in " << paramName <<
                " paramater: " << *str;
            return 0;
        }
        ret *= 10;
        ret += (*str++ - '0');
    }

    return ret;
}

typedef struct growbuffer 
{
    // The total number of bytes, and the start byte
    int size;
    // The start byte
    int start;
    // The blocks
    char data[64 * 1024];
    struct growbuffer *prev, *next;
} growbuffer;

// returns nonzero on success, zero on out of memory
static int growbuffer_append(growbuffer **gb, const char *data, int dataLen) {
    while(dataLen) {
        growbuffer *buf = *gb ? (*gb)->prev : 0;
        if(!buf || (buf->size == sizeof(buf->data))) {
            buf = (growbuffer *) malloc(sizeof(growbuffer));
            if(!buf) {
                return 0;
            }
            buf->size = 0;
            buf->start = 0;
            if (*gb) {
                buf->prev = (*gb)->prev;
                buf->next = *gb;
                (*gb)->prev->next = buf;
                (*gb)->prev = buf;
            }
            else {
                buf->prev = buf->next = buf;
                *gb = buf;
            }
        }

        int toCopy = (sizeof(buf->data) - buf->size);
        if(toCopy > dataLen) {
            toCopy = dataLen;
        }

        memcpy(&(buf->data[buf->size]), data, toCopy);
        
        buf->size += toCopy, data += toCopy, dataLen -= toCopy;
    }
    return 1;
}

static void growbuffer_read(growbuffer **gb, int amt, int *amtReturn, char *buffer) {
    *amtReturn = 0;

    growbuffer *buf = *gb;

    if(!buf) {
        return;
    }

    *amtReturn = (buf->size > amt) ? amt : buf->size;

    memcpy(buffer, &(buf->data[buf->start]), *amtReturn);

    buf->start += *amtReturn, buf->size -= *amtReturn;

    if(buf->size == 0) {
        if(buf->next == buf) {
            *gb = 0;
        }
        else {
            *gb = buf->next;
            buf->prev->next = buf->next;
            buf->next->prev = buf->prev;
        }
        free(buf);
    }
}

static void growbuffer_destroy(growbuffer *gb) {
    growbuffer *start = gb;
    while(gb) {
        growbuffer *next = gb->next;
        free(gb);
        gb = (next == start) ? 0 : next;
    }
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
    return 1;
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

// list bucket -----------------------------------------------------------------------------------------

typedef struct list_bucket_callback_data
{
    int isTruncated;
    char nextMarker[1024];
    int keyCount;
    int allDetails;
} list_bucket_callback_data;


static S3Status listBucketCallback(int isTruncated, const char* nextMarker, int contentsCount,
        const S3ListBucketContent *contents, int commonPrefixesCount, const char **commonPrefixes,
        void *callbackData) {
    list_bucket_callback_data *data = (list_bucket_callback_data *) callbackData;

    data->isTruncated;
    // This is tricky.  S3 doesn't return the NextMarker if there is no
    // delimiter.  Why, I don't know, since it's useful for paging through
    // results.  We want NextMarker to be the last content in the list,
    // so set it to that if necessary.
    if((!nextMarker || !nextMarker[0]) && contentsCount) {
        nextMarker = contents[contentsCount -1].key;
    }
    if(nextMarker) {
        snprintf(data->nextMarker, sizeof(data->nextMarker), "%s",
                nextMarker);
    }
    else {
        data->nextMarker[0] = 0;
    }
    
    if(contentsCount && !data->keyCount) {
        //printListBucketHeader(data->allDetails);
    }

    int i;
    for(i = 0; i < contentsCount; i++) {
        const S3ListBucketContent *content = &(contents[i]);
        char timebuf[256];
        if (0) {
            time_t t = (time_t) content->lastModified;
            strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
                    gmtime(&t));
            Rcout << "\nKey: " << content->key << "\n";
            Rcout << "Lst Modified: " << timebuf << "\n";
            Rcout << "Etag: " << content->eTag << "\n";
            Rcout << "Size: " << (unsigned long long) content->size << "\n";
            if(content->ownerId) {
                Rcout << "Owner ID: " << content->ownerId << "\n";
            }
            if(content->ownerDisplayName) {
                Rcout << "Owner Display Name: " << content->ownerDisplayName << "\n";
            }
        }
        else {
            time_t t = (time_t) content->lastModified;
            strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
                    gmtime(&t));
            char sizebuf[16];
            if(content->size < 100000) {
                sprintf(sizebuf, "%4lluK",
                        ((unsigned long long) content->size) / 1024ULL);
            }
            else if (content->size < (10 * 1024 * 1024)) {
                float f = content->size;
                f /= (1024 * 1024);
                sprintf(sizebuf, "%1.2fM", f);
            }
            else if (content->size < (1024 * 1024 * 1024)) {
                sprintf(sizebuf, "%4lluM",
                        ((unsigned long long) content->size) /
                        (1024ULL * 1024ULL));
            }
            else {
                float f = (content->size / 1024);
                f /= (1024 * 1024);
                sprintf(sizebuf, "%1.2fG", f);
            }
            Rcout << content->key << " " << timebuf << " " << sizebuf;
            if (data->allDetails) {
                //Rcout << "   " << content->eTag  << "  " << content->ownerId ? content->ownerId : "" \
                    << content->ownerDisplayName ? content->ownerDisplayName : "";
            }
            Rcout << "\n";
        }
    }

    data->keyCount += contentsCount;
    
    for(i = 0; i < commonPrefixesCount; i++) {
        Rcout << "\nCommon Prefix:  " << commonPrefixes[i];
    }

    return S3StatusOK;
}




// [[Rcpp::export]]
int list_bucket(const char* bucketName, const char* prefix = "", int allDetails = 0) {

    S3_init();

    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };
    


    S3ListBucketHandler listBucketHandler =
    {
        { &responsePropertiesCallback, &responseCompleteCallback},
        &listBucketCallback
    };

    // temp put things here
    // Can eventually move to function
    const char *marker = 0;
    const char *delimiter = 0;
    int maxkeys = 0;

    list_bucket_callback_data data;

    snprintf(data.nextMarker, sizeof(data.nextMarker), "%s", marker);
    data.keyCount = 0;
    data.allDetails = allDetails;

    do {
        data.isTruncated = 0;
        do {
            S3_list_bucket(&bucketContext, prefix, data.nextMarker,
                    delimiter, maxkeys, 0, &listBucketHandler, &data);
        } while(S3_status_is_retryable(statusG) && should_retry());
        if(statusG != S3StatusOK) {
            break;
        }
    } while(data.isTruncated && (!maxkeys || (data.keyCount < maxkeys)));

    if(statusG == S3StatusOK) {
        if(!data.keyCount) {
            //printListBucketHeader(allDetails);
            Rcout << "here it would print\n";
        }
    }
    else {
        printError();
    }

    S3_deinitialize();

}

// delete object ------------------------------------------------------------------------

// [[Rcpp::export]]
int delete_object(const char* bucketName, const char* key) {

    S3_init();

    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };
    

    S3ResponseHandler responseHandler =
    {
        0,
        &responseCompleteCallback
    };

    do {
        S3_delete_object(&bucketContext, key, 0, &responseHandler, 0);
    } while(S3_status_is_retryable(statusG) && should_retry());

    if ((statusG != S3StatusOK) &&
            (statusG != S3StatusErrorPreconditionFailed)) {
        printError();
    }

    S3_deinitialize();
    return 1;

}

// put object ------------------------------------------------------------------------

typedef struct put_object_callback_data
{
    FILE *infile;
    growbuffer *gb;
    uint64_t contentLength, originalContentLength;
    int noStatus;
} put_object_callback_data;

static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData) {
    put_object_callback_data *data = (put_object_callback_data *) callbackData;

    int ret = 0;

    if (data->contentLength) {
        int toRead = ((data->contentLength > (unsigned) bufferSize) ?
                (unsigned) bufferSize : data->contentLength);
        if(data->gb) {
            growbuffer_read(&(data->gb), toRead, &ret, buffer);
        }
        else if(data->infile) {
            ret = fread(buffer, 1, toRead, data->infile);
        }
    }

    data->contentLength -= ret;

    if(data->contentLength && !data->noStatus) {
        // Avoid a weird bug in MingW, which won't print the second integer
        // value properly when it's in the same call, so print separately
        Rcout << (unsigned long long) data->contentLength <<  "  bytes remaining ";
        Rcout << "(" << (int) (((data->originalContentLength - data->contentLength) * 100)
                    / data->originalContentLength) << " complete) ...\n";
    }

    return ret;
}

// [[Rcpp::export]]
int put_object(const char* bucketName, const char* storage_location, const char* filename) {
    Rcout << "Test5";

    //char *slash;
    // Split bucket/key
    //*slash++ = 0;

    const char *key = storage_location;
    
    const char* contentType = 0;

    uint64_t contentLength = 0;
    const char *cacheControl = 0, *md5 = 0;
    const char *contentDispositionFilename = 0, *contentEncoding = 0;
    int64_t expires = -1;
    S3CannedAcl cannedAcl = S3CannedAclPrivate;
    int metaPropertiesCount = 0;
    S3NameValue metaProperties[S3_MAX_METADATA_COUNT];
    char useServerSideEncryption = 0;
    int noStatus = 0;
    Rcout << "Test1";

    put_object_callback_data data;

    data.infile = 0;
    data.gb = 0;
    data.noStatus = noStatus;

    Rcout << "Test2";
    if(filename) {
        if(!contentLength) {
            struct stat statbuf;
            // Stat the file to get its length
            if(stat(filename, &statbuf) == -1) {
                Rcout << "\nERROR:  Failed to get file length of file:  " << filename;
                perror(0);
                return 0;
            }
            contentLength = statbuf.st_size;
        }
        if(!(data.infile = fopen(filename, "r" FOPEN_EXTRA_FLAGS))) {
            Rcout << "\nERROR:  Failed to open input file:  " << filename;
            perror(0);
            return 0;
        }
    }
    Rcout << "Test3";

    data.contentLength = data.originalContentLength = contentLength;

    S3_init();
    
    S3BucketContext bucketContext =
    {
        0,
        bucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };

    S3PutProperties putProperties =
    {
        contentType,
        md5,
        cacheControl,
        contentDispositionFilename,
        contentEncoding,
        expires,
        cannedAcl,
        metaPropertiesCount,
        metaProperties
            //unsure why useServerSideEncryption doesn't work
        //useServerSideEncryption
    };

    S3PutObjectHandler putObjectHandler =
    {
        { &responsePropertiesCallback, &responseCompleteCallback },
        &putObjectDataCallback
    };

    do {
        S3_put_object(&bucketContext, key, contentLength, &putProperties, 0,
                &putObjectHandler, &data);
    } while(S3_status_is_retryable(statusG) && should_retry());

    if(data.infile) {
        fclose(data.infile);
    }
    else if(data.gb) {
        growbuffer_destroy(data.gb);
    }

    if(statusG != S3StatusOK) {
        printError();
    }
    else if(data.contentLength) {
        Rcout << "\nERROR: Failed to read remaining " << (unsigned long long) data.contentLength << " bytes from "
            "input\n";
    }

    S3_deinitialize();
    return 1;
}

// copy object ----------------------------------------------------------------------------------------------


// [[Rcpp::export]]
int copy_object(const char* sourceBucketName, const char* sourceKey,
        const char* destinationBucketName, const char* destinationKey) {
    
    const char *cacheControl = 0, *contentType = 0;
    const char *contentDispositionFilename = 0, *contentEncoding = 0;
    int64_t expires = -1;
    S3CannedAcl cannedAcl = S3CannedAclPrivate;
    int metaPropertiesCount = 0;
    S3NameValue metaProperties[S3_MAX_METADATA_COUNT];
    char useServerSideEncryption = 0;
    int anyPropertiesSet = 0;

    S3_init();

    S3BucketContext bucketContext =
    {
        0,
        sourceBucketName,
        protocolG,
        uriStyleG,
        accessKeyIdG,
        secretAccessKeyG
    };

    S3PutProperties putProperties =
    {
        contentType,
        0,
        cacheControl,
        contentDispositionFilename,
        contentEncoding,
        expires,
        cannedAcl,
        metaPropertiesCount,
        metaProperties
        //useServerSideEncryption
    };

    S3ResponseHandler responseHandler =
    {
        &responsePropertiesCallback,
        &responseCompleteCallback
    };

    int64_t lastModified;
    char eTag[256];

    do {
        S3_copy_object(&bucketContext, sourceKey, destinationBucketName, destinationKey, anyPropertiesSet ? &putProperties : 0,
                &lastModified, sizeof(eTag), eTag, 0, &responseHandler, 0);
    } while(S3_status_is_retryable(statusG) && should_retry());

    if(statusG == S3StatusOK) {
        if(lastModified >= 0) {
            char timebuf[256];
            time_t t = (time_t) lastModified;
            strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
                    gmtime(&t));
            Rcout << "Last-Modified: " << timebuf;
        }
        if (eTag[0]) {
            Rcout << "ETag:  " << eTag;
        }
    }
    else {
        printError();
    }

    S3_deinitialize();
    return 1;
}

