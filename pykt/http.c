#include "http.h"
#include "response.h"

#define BUF_SIZE 1024 * 64

static inline int 
connect_socket(http_connection *con);

static inline int 
recv_data(http_connection *con);

static inline int 
recv_response(http_connection *con, int status_code);

static inline int
call_select(http_connection *con, int write)
{
    int ret;
    fd_set fds;
    struct timeval tv;
    int fd = con->fd;
    int timeout = con->timeout;

    if (timeout <= 0){
        return 0;
    }
    if (fd < 0){
        return 0;
    }

    tv.tv_sec = timeout;
    tv.tv_usec = (int)((timeout - tv.tv_sec) * 1e6);
    FD_ZERO(&fds);
    FD_SET(fd, &fds);

    if (write){
        ret = select(fd + 1, NULL, &fds, NULL, &tv);
    }else{
        ret = select(fd + 1, &fds, NULL, NULL, &tv);
    }
    if (ret < 0){
        return -1;
    }
    if (ret == 0){
        return 1;
    }
    return 0;
}

static inline int 
writev_bucket(http_connection *con)
{
    int ret = 0;
    int i = 0, timeout = 0;
    data_bucket *bucket = con->bucket;
    
    DEBUG("writev_bucket total_size:%d iov_cnt:%d", bucket->total_size, bucket->iov_cnt);

    Py_BEGIN_ALLOW_THREADS
    if(wait_callback){ 
        ret = writev(con->fd, bucket->iov, bucket->iov_cnt);
    }else{
        timeout = call_select(con, 1);
        if(!timeout){
            ret = writev(con->fd, bucket->iov, bucket->iov_cnt);
        }
    }
    Py_END_ALLOW_THREADS
    
    if(timeout == 1){
        PyErr_SetString(TimeoutException, "timed out");
        return -1; 
    }

    switch(ret){
        case 0:
            bucket->sended = 1;
            return 1;
        case -1:
            //error
            if (errno == EAGAIN || errno == EWOULDBLOCK) { 
                // try again later
                return 0;
            }else{
                //ERROR
                PyErr_SetFromErrno(PyExc_IOError);
                return -1;
            }
            break;
        default:
            if(bucket->total > ret){
                for(; i < bucket->iov_cnt;i++){
                    if(ret > bucket->iov[i].iov_len){
                        //already write
                        ret -= bucket->iov[i].iov_len;
                        bucket->iov[i].iov_len = 0;
                    }else{
                        bucket->iov[i].iov_base += ret;
                        bucket->iov[i].iov_len = bucket->iov[i].iov_len - ret;
                        break;
                    }
                }
                bucket->total = bucket->total - ret;
                return writev_bucket(con);
            }
            bucket->sended = 1;

    }

    return 1;
}

inline http_connection *
open_http_connection(char *host, int port, int timeout)
{

    http_connection *con = NULL;
    int fd = -1;
    DEBUG("open_http_connection args %s:%d timeout:%d", host, port, timeout);

    con = PyMem_Malloc(sizeof(http_connection));
    if(con == NULL){
        PyErr_NoMemory();
        return NULL;
    }
    memset(con, 0, sizeof(http_connection));
    con->fd = -1;
    con->bucket = NULL;
    con->response_status = RES_INIT;
    con->head = 0;
    con->have_kt_error = 0;
    con->status_code = 0;
    con->host = host;
    con->port = port;
    con->timeout = timeout;

    DEBUG("open_http_connection new %p", con);

    fd = connect_socket(con);
    if(fd < 0){
        if(con){
            PyMem_Free(con);
        }
        return NULL;
    }
    
    con->fd = fd;
    DEBUG("open http_connection connected %p fd:%d", con, con->fd);
    return con;
}

inline void
free_http_data(http_connection *con)
{
    if(con == NULL){
        return;
    }
    DEBUG("free_http_data %p", con);
    if(con->bucket){
        free_data_bucket(con->bucket);
        con->bucket = NULL;
    }
    if(con->response_body){
        free_buffer(con->response_body);
        con->response_body = NULL;
    }
}

inline int
close_http_connection(http_connection *con)
{

    int ret = 0;
    if(con == NULL){
        return 0;
    }
    DEBUG("close_http_connection %p, fd: %d", con, con->fd);
    
    free_http_data(con);

    if(con->fd >= 0){
        close(con->fd);
        DEBUG("close con fd:%d", con->fd);
        con->fd = -1;
        ret = 1;
    }
    PyMem_Free(con);
    return ret;
}

static inline int
internal_connect(http_connection *con, const struct sockaddr *addr, socklen_t addrlen, int *timeup)
{
    int res, timeout = 0;

    res = connect(con->fd, addr, addrlen);
    if(con->timeout > 0){
        if(res < 0 && errno == EINPROGRESS && con->fd < FD_SETSIZE){
            timeout = call_select(con, 1);
            if (timeout == 0){
                socklen_t res_size = sizeof(res);
                (void)getsockopt(con->fd, SOL_SOCKET, SO_ERROR, &res, &res_size);
                if (res == EISCONN){
                    res = 0;
                }
                errno = res;
            }else if (timeout == -1){
                //select error
                res = errno;
            }else{
                //timeout
                res = EWOULDBLOCK;
            }
        }
    }
    if(res < 0){
        res = errno;
    }
    *timeup = timeout;
    return res;
}


static inline int 
connect_socket(http_connection *con)
{
    struct addrinfo hints, *res = NULL, *ai = NULL;
    int flag = 1, err, fd = -1;
    char strport[7];
    int timeup = 0;

    DEBUG("connect_socket %s:%d timeout:%d", con->host, con->port, con->timeout);
   
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; 
    
    snprintf(strport, sizeof(strport), "%d", con->port);
    
    Py_BEGIN_ALLOW_THREADS
    err = getaddrinfo(con->host, strport, &hints, &res);
    Py_END_ALLOW_THREADS

    if (err == -1) {
        //DEBUG("error getaddrinfo");
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    for(ai = res; ai != NULL; ai = ai->ai_next) {
        
        Py_BEGIN_ALLOW_THREADS
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        Py_END_ALLOW_THREADS
        
        if (fd == -1){
            continue;
        }

        if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag,
                sizeof(int)) == -1) {
            close(fd);
            PyErr_SetFromErrno(PyExc_IOError);
            goto error;
        }
        
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag,
                sizeof(int)) == -1) {
            close(fd);
            PyErr_SetFromErrno(PyExc_IOError);
            goto error;
        }
        
        if(con->timeout > 0){
            // set non_blocking
            if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1){
                close(fd);
                PyErr_SetFromErrno(PyExc_IOError);
                goto error;
            }
        }
        con->fd = fd;
        Py_BEGIN_ALLOW_THREADS
        //err = connect(fd, ai->ai_addr, ai->ai_addrlen);
        err = internal_connect(con, ai->ai_addr, ai->ai_addrlen, &timeup);
        Py_END_ALLOW_THREADS
        
        if (timeup == 1){
            close(fd);
            fd = -1;
            continue;
        }
        if (err != 0) {
            close(fd);
            fd = -1;
            continue;
        }
        break;
    }

    if (ai == NULL)  {
        close(fd);
        PyErr_SetString(PyExc_IOError,"failed to connect\n");
        goto error;
    }

    if(wait_callback){
        if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1){
            close(fd);
            PyErr_SetFromErrno(PyExc_IOError);
            goto error;
        }
    }

    freeaddrinfo(res);
    DEBUG("connect success %d", fd);
    return fd;
error:
    if(res){
        freeaddrinfo(res);
    }
    con->fd = -1;
    return -1;
}

inline int  
request(http_connection *con, int status_code)
{
   int ret;

   DEBUG("request http_connection %p", con);

   ret = send_data(con);
   
   if(ret < 0){
       //error
       return ret;
   }
   return recv_response(con, status_code);
}

inline int 
send_data(http_connection *con)
{
    int ret;
    
    ret = writev_bucket(con);
    switch(ret){
        case 0:
            //EWOULDBLOCK or EAGAIN
            call_wait_callback(con->fd, WAIT_WRITE);
            return send_data(con);
        case -1:
            //IO Error
            return -1;
        default:
            break;
    }
    DEBUG("sended request data. http_connection %p", con);
    return 1;
}

static inline int 
recv_response(http_connection *con, int status_code)
{
    http_parser *parser;
    int ret;
    
    DEBUG("recv_response http_connection %p", con);

    parser = init_parser(con);
    if(parser == NULL){
        //alloc error
        return -1;
    }
    con->response_status = RES_READY;     
    while(1){
        ret = recv_data(con);
        if(ret > 0){
            //complete
            break;
        }
        if(ret < 0){
            //TODO Error
            goto error;
        }
    }
    
    DEBUG("response status code %d", parser->status_code);
    
    if(parser->status_code == 400 ){
        //Invalid parameter
        PyErr_SetString(KtException,"Invalid parameter");
        goto error;
    }
    if(parser->status_code != status_code){
        
        goto error;
    }
    if(con->parser){
        PyMem_Free(con->parser);
        con->parser = NULL;
    }
    
    return 1;
error:
    if(con->parser){
        PyMem_Free(con->parser);
        con->parser = NULL;
    }
    return -1;
}

static inline int 
recv_data(http_connection *con)
{
    char buf[BUF_SIZE];
    ssize_t r = 0;
    int nread, timeout = 0;
    
    Py_BEGIN_ALLOW_THREADS
    if(wait_callback){
        r = read(con->fd, buf, sizeof(buf));
    }else{
        timeout = call_select(con, 0);
        if(!timeout){
            r = read(con->fd, buf, sizeof(buf));
        }
    }
    Py_END_ALLOW_THREADS
    
    //timeout
    if(timeout == 1){
        PyErr_SetString(TimeoutException, "timed out");
        con->response_status = RES_HTTP_ERROR;
        return -1;
    }

    //response dump
    DUMP("read data fd:%d read:%d \n%.*s", con->fd, r, r, buf);

    switch(r){
        case 0:
            //close  
            PyErr_SetString(PyExc_IOError,"connection closed");
            con->response_status = RES_HTTP_ERROR;
            return -1;
        case -1:
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                call_wait_callback(con->fd, WAIT_READ);
                return 0;
            } else {
                PyErr_SetFromErrno(PyExc_IOError);
            }
            con->response_status = RES_HTTP_ERROR;
            return -1;
        default:
            break;
    }
    nread = execute_parse(con, buf, r);
    
    if(con->response_status == RES_KT_ERROR){
        return -1;
    }
    
    if(nread != r){
        PyErr_SetString(PyExc_IOError,"HTTP response parse error");
        con->response_status = RES_HTTP_ERROR;
        return -1;
    }
    
    switch(con->response_status){
        case RES_SUCCESS:
            return 1;
        case RES_READY:
            return 0;
        default:
            return -1;
    }
    return 0;

}
