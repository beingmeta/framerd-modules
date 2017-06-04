;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'aws/sns)

(use-module '{aws aws/v4 texttools logger fdweb varconfig})
(define %used_modules '{aws varconfig})

(define-init %loglevel %notice%)

(module-export! '{sns/newtopic sns/topic 
		  sns/publish 
		  sns/subscribe! sns/confirm! sns/unsubscribe!
		  sns/permit! sns/unpermit!})

(define base-opts #[])

(define (get-endpoint opts)
  (if (string? opts)
      (glom "https://sns." opts ".amazonaws.com/")
      (getopt opts 'endpoint
	      (or sns-endpoint
		  (glom "https://sns." 
		    (getopt opts 'region aws:region)
		    ".amazonaws.com/")))))

(define (arn-region arn)
  (get (text->frames #("arn:aws:sns:" (label region (not> ":"))) arn)
       'region))

(define sns-endpoint #f)
(varconfig! sns:endpoint sns-endpoint)

(define (sns/newtopic name (opts base-opts))
  (let* ((req (aws/v4/get #[] (get-endpoint opts) opts
			  `#["Action" "CreateTopic" "Name" ,name]))
	 (text (and (pair? req)
		    (response/ok? (car req))
		    (getopt req '%content)))
	 (parsed (and text (xmlparse text))))
    (and parsed (xmlcontent (xmlget parsed 'topicarn)))))

;; We should probably have a database of topic names to allow this to
;; check that a topic name is valid, since sns/newtopic will create
;; the topic if it doesn't exist.
(define (sns/topic name (create #t))
  (cond ((not (string? name)) 
	 (irritant name |NotAnSNSTopic| sns/topic))
	((has-prefix name "arn:aws:sns:") name)
	;; This is where we should check if it's a valid topic name
	((not create) (error NYI sns/topic "Probe calls to sns/topic are not available"))
	(else (sns/newtopic name))))

(define (sns/publish topic-arg message (opts base-opts))
  (let* ((arn (sns/topic topic-arg)))
    (aws/v4/get #[] (get-endpoint opts) opts
		`#["Action" "Publish" "Message" ,message "TopicArn" ,arn])))

(define (sns/subscribe! topic-arg endpoint (opts base-opts))
  (let* ((arn (sns/topic topic-arg))
	 (protocol (get-protocol endpoint))
	 (args `#["Action" "Subscribe" 
		  "Endpoint" ,endpoint
		  "Protocol" ,protocol
		  "TopicArn" ,arn])
	 (response (aws/v4/get #[] (get-endpoint opts) opts args)))
    response))

(define (sns/confirm! topic-arg token (opts base-opts))
  (let* ((arn (sns/topic topic-arg))
	 (endpoint (if (string? opts) opts (get-endpoint opts)))
	 (protocol (get-protocol endpoint))
	 (args `#["Action" "ConfirmSubscription" 
		  "Token" ,token "TopicArn" ,arn])
	 (response (aws/v4/get #[] (get-endpoint opts) opts args)))
    response))

(define (get-protocol string)
  (cond ((has-prefix string "http:") "http")
	((has-prefix string "https:") "https")
	(else (irritant string |UnhandledProtocol|))))

(define (sns/unsubscribe! arn (opts base-opts))
  (aws/v4/get #[] (get-endpoint opts) opts `#["Action" "Unsubscribe" ,arn]))

(defambda (sns/permit! topic-arg actions accounts (opts))
  (for-choices topic-arg
    (let* ((arn (sns/topic topic-arg))
	   (args `#["Action" "AddPermission" "TopicArn" ,arn
		    "Label" ,(getopt opts 'label (glom (config 'appid) (get (gmtimestamp) 'isobasic)))]))
      (do-choices (action actions i)
	(store! args (glom "ActionName.member." (1+ i)) action))
      (do-choices (account accounts i)
	(store! args (glom "AWSAccountId.member." (1+ i)) account))
      (aws/v4/get #[] (get-endpoint opts) opts args))))

(defambda (sns/unpermit! topic-arg label (opts base-opts))
  (for-choices topic-arg
    (let* ((arn (sns/topic topic-arg))
	   (args `#["Action" "RemovePermission" "TopicArn" ,arn "Label" ,label]))
      (aws/v4/get #[] (get-endpoint opts) opts args))))




