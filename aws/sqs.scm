;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'aws/sqs)

(use-module '{aws aws/v4 texttools logger varconfig})
(define %used_modules '{aws varconfig})

(module-export! '{sqs/get sqs/list sqs/info
		  sqs/send! sqs/delete!
		  sqs/send sqs/delete
		  sqs/extend sqs/req/extend
		  sqs/getn sqs/vacuum})

(define-init %loglevel %notice%)

(define sqs-endpoint "https://sqs.us-east-1.amazonaws.com/")
(varconfig! sqs:endpoint sqs-endpoint)

(define (sqs-field-pattern name (label) (parser #f))
  (default! label (string->lisp name))
  `#(,(glom "<" name ">")
     (label ,label (not> ,(glom "</" name ">")) ,parser)
     ,(glom "</" name ">")))
(define (sqs-attrib-pattern name (label) (parser #f))
  (default! label (string->lisp name))
  `#("<Attribute><Name>" ,name "</Name><Value>"
     (label ,label (not> "</Value>") ,parser)
     "</Value></Attribute>"))

(define queue-opts (make-hashtable))

(define sqs-fields
  {(sqs-field-pattern "Body")
   (sqs-field-pattern "ReceiptHandle" 'handle)
   (sqs-field-pattern "MessageId" 'msgid)
   (sqs-field-pattern "RequestId" 'reqid)
   (sqs-field-pattern "QueueUrl" 'queue)
   (sqs-field-pattern "SenderId" 'sender)
   (sqs-field-pattern "SentTimestamp" 'sent)
   (sqs-field-pattern "ApproximateReceiveCount")
   (sqs-field-pattern "ApproximateFirstReceiveTimestamp")})

(define (timevalue x) (timestamp (->number x)))

(define sqs-info-fields
  {(sqs-attrib-pattern "ApproximateNumberOfMessages" 'count #t)
   (sqs-attrib-pattern "ApproximateNumberOfMessagesNotVisible" 'inflight #t)
   (sqs-attrib-pattern "ApproximateNumberOfMessagesDelayed" 'delayed #t)
   (sqs-attrib-pattern "VisibilityTimeout" 'interval #t)
   (sqs-attrib-pattern "CreatedTimestamp" 'created timevalue)
   (sqs-attrib-pattern "LastModifiedTimestamp" 'modified timevalue)
   (sqs-attrib-pattern "Policy")
   (sqs-attrib-pattern "MaximumMessageSize" 'maxmsg #t)
   (sqs-attrib-pattern "MessageRetentionPeriod" 'retention #t)
   (sqs-attrib-pattern "QueueArn" 'arn #f)
   (sqs-attrib-pattern "ReceiveMessageWaitTimeSeconds" 'timeout #t)
   (sqs-attrib-pattern "DelaySeconds" 'delay #t)})

(define (handle-sqs-response result (extract (qc sqs-fields)))
  (debug%watch "handle-sqs-response" result)
  (if (and result (table? result) (test result 'response)
	   (>= 299 (get result 'response) 200))
      (let* ((combined (frame-create #f
			 'queue (getopt result '%queue {})
			 'received (gmtimestamp)))
	     (content (getopt result '%content))
	     (fields (tryif content (text->frames extract content))))
	(if (or (fail? fields) (fail? (get fields 'msgid)))
	    (begin (debug%watch content fields) #f)
	    (begin (debug%watch fields)
	      (do-choices (field fields)
		(do-choices (key (getkeys field))
		  (add! combined key (get field key))))
	      combined)))
      (begin (logwarn |SQSFailure| result)
	#f)))

(define (handle-sqs-error ex method queue)
  (if (error-irritant? ex)
      (irritant (error-irritant ex) |SQS/Failed|
	"Received from " method " " queue "@" (getopt (error-irritant ex) 'effective-url))
      (error |SQS/Failed| "Received from " method " " queue)))

(define (get-queue-opts (queue #f) (opts #[]) (qopts))
  (default! qopts (try (tryif queue (get queue-opts queue)) #[]))
  (frame-create #f '%queue (tryif queue queue) 'err #f))

(define (sqs/get queue (opts #[])
		 (args `#["Action" "ReceiveMessage" "AttributeName.1" "all"]))
  (when (getopt opts 'wait)
    (store! args "WaitTimeSeconds" (getopt opts 'wait)))
  (when (getopt opts 'reserve)
    (store! args "VisibilityTimeout" (getopt opts 'reserve)))
  (handle-sqs-response 
   (aws/v4/op (get-queue-opts queue opts)
	      "GET" queue opts args)))

(define (sqs/send! queue msg (opts #[]) (args `#["Action" "SendMessage"]))
  (store! args "MessageBody" msg)
  (when (getopt opts 'delay) (store! args "DelaySeconds" (getopt opts 'delay)))
  (onerror
      (handle-sqs-response 
       (aws/v4/get (get-queue-opts queue opts) queue opts args))
    (lambda (ex)
      (handle-sqs-error ex '|SendMessage| queue))))
(define (sqs/send queue msg (opts #[]) (args `#["Action" "SendMessage"]))
  (sqs/send! queue msg opts args))

(define (sqs/list (prefix #f) (args #["Action" "ListQueues"]) (opts #[]))
  (when prefix (set! args `#["Action" "ListQueues" "QueueNamePrefix" ,prefix]))
  (onerror
      (handle-sqs-response 
       (aws/v4/get (get-queue-opts #f opts) sqs-endpoint opts args))
    (lambda (ex)
      (handle-sqs-error ex '|ListQueues| #f))))

(define (sqs/info queue
		  (args #["Action" "GetQueueAttributes" "AttributeName.1" "All"])
		  (opts #[]))
  (onerror
      (handle-sqs-response
       (aws/v4/get (get-queue-opts queue opts) queue opts args)
       (qc sqs-info-fields))
    (lambda (ex)
      (handle-sqs-error ex '|GetQueueAttributes| queue))))

(define (sqs/delete! message (opts #[]))
  (onerror
      (handle-sqs-response
       (aws/v4/get (get-queue-opts (get message 'queue))
		   (get message 'queue) opts
		   `#["Action" "DeleteMessage" "ReceiptHandle" ,(get message 'handle)]))
    (lambda (ex)
      (handle-sqs-error ex '|DeleteMessage| (get message 'queue)))))
(define (sqs/delete message (opts #[])) (sqs/delete! message opts))

(define (sqs/extend message secs (opts #[]))
  (onerror
      (handle-sqs-response
       (aws/v4/get (get-queue-opts (get message 'queue))
		   (get message 'queue) opts
		   `#["Action" "ChangeMessageVisibility"
		      "ReceiptHandle" ,(get message 'handle)
		      "VisibilityTimeout" ,secs]))
    (lambda (ex)
      (handle-sqs-error ex '|ExtendMessage| (get message 'queue)))))

(define reqvar '_sqs)
(define default-extension 60)
(varconfig! sqs:extension default-extension)
(varconfig! sqs:reqvar reqvar)

(define (sqs/req/extend (secs #f))
  (when (req/test reqvar)
    (let ((entry (req/get reqvar)))
      (and (or (pair? entry) (table? entry))
	   (testopt entry 'handle) (testopt entry 'queue)
	   (sqs/extend `#[handle ,(getopt entry 'handle)
			  queue ,(getopt entry 'queue)]
		       (or secs (getopt entry 'extension
					default-extension)))))))

;;; GETN returns multiple items from a queue (#f means all)

(define (sqs/getn queue (n #f))
  (let ((item (sqs/get queue)) (count 0) (result {})
	(seen (make-hashset)))
    (until (or (not item) (and n (>= count n)))
      (unless (get seen (get item 'msgid))
	(hashset-add! seen (get item 'msgid))
	(set+! result item))
      (set! count (1+ count))
      (set! item (sqs/get queue)))
    result))

;;; Vacuuming removes all the entries from a queue

(define (sqs/vacuum queue)
  (let ((item (sqs/get queue)) (count 0))
    (while item
      (sqs/delete item)
      (set! count (1+ count))
      (set! item (sqs/get queue)))
    count))
