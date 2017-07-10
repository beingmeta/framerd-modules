;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'aws/sns)

(use-module '{aws aws/v4 texttools logger fdweb varconfig})
(define %used_modules '{aws varconfig})

(define-init %loglevel %warn%)

(module-export! '{sns/newtopic sns/topic 
		  sns/publish 
		  sns/subscribe! sns/confirm! sns/unsubscribe!
		  sns/permit! sns/unpermit!})

(define base-opts #[])

(define-init subscriptions (make-hashtable))
(define-init subscribe-wait 2000)

(define use-sns-endpoint #f)
(varconfig! sns:endpoint use-sns-endpoint)

(define (sns-endpoint opts)
  (if (string? opts)
      (glom "https://sns." opts ".amazonaws.com/")
      (getopt opts 'endpoint
	      (or use-sns-endpoint
		  (glom "https://sns." 
		    (getopt opts 'region aws:region)
		    ".amazonaws.com/")))))

(define (sns-protocol string)
  (cond ((has-prefix string "http:") "http")
	((has-prefix string "https:") "https")
	(else (irritant string |UnhandledProtocol|))))

(define (arn-region arn)
  (get (text->frames #("arn:aws:sns:" (label region (not> ":"))) arn)
       'region))

(define (sns/newtopic name (opts base-opts))
  (let* ((req (aws/v4/get #[] (sns-endpoint opts) opts
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
    (aws/v4/get #[] (sns-endpoint opts) opts
		`#["Action" "Publish" "Message" ,message "TopicArn" ,arn])))

(define (sns/subscribe! topic-arg endpoint (opts base-opts) (arn))
  (default! arn (sns/topic topic-arg))
  (or (getopt (get subscriptions (cons arn endpoint)) 'subscription)
      (do-subscribe arn endpoint opts)))

(defslambda (do-subscribe arn endpoint opts)
  (or (getopt (get subscriptions (cons arn endpoint)) 'subscription)
      (let* ((args `#["Action" "Subscribe" 
		      "Endpoint" ,endpoint
		      "Protocol" ,(sns-protocol endpoint)
		      "TopicArn" ,arn])
	     (response (aws/v4/get #[] (sns-endpoint opts) opts args)))
	;; TODO: Should handle subscriptions which are immediately returned
	(store! subscriptions arn 
		`#[topic ,arn endpoint ,endpoint
		   requested ,(timestamp)])
	(if (pair? response) (car response) response)
	(lognotice |SNS/Subscribe| 
	  "Requested subscription to " arn " for " endpoint)
	(info%watch "SNS/Subscribe/response" response)
	response)))

(define (sns/confirm! topic-arg endpoint token (opts base-opts))
  (let* ((arn (sns/topic topic-arg)))
    (unless (test (get subscriptions (cons arn endpoint)) 'token token)
      (let* ((protocol (sns-protocol endpoint))
	     (args `#["Action" "ConfirmSubscription" 
		      "Token" ,token "TopicArn" ,arn])
	     (response (aws/v4/get #[] (sns-endpoint opts) opts args))
	     (parsed (xmlparse (getopt response '%content)))
	     (subarn (xmlget parsed 'subscriptionarn)))
	(store! subscriptions (cons arn endpoint)
		`#[subscription ,subarn 
		   topic ,arn endpoint ,endpoint
		   token ,token])
	(lognotice |SNS/Confirm|
	  "Confirmed subscription to " arn " with " token ":\n\t" subarn)
	(info%watch "SNS/Confirm/response" arn subarn token response)
	response))))

(define (sns/unsubscribe! arn (opts base-opts))
  (let ((response (aws/v4/get #[] (sns-endpoint opts) opts
			      `#["Action" "Unsubscribe" "SubscriptionArn" ,arn])))
    (lognotice |SNS/Unsubscribe| "Confirmed subscription to " arn)
    (info%watch "SNS/Unsubscribe/response" response)
    response))

(defambda (sns/permit! topic-arg actions accounts (opts))
  (for-choices topic-arg
    (let* ((arn (sns/topic topic-arg))
	   (args `#["Action" "AddPermission" "TopicArn" ,arn
		    "Label" ,(getopt opts 'label (glom (config 'appid) (get (gmtimestamp) 'isobasic)))]))
      (do-choices (action actions i)
	(store! args (glom "ActionName.member." (1+ i)) action))
      (do-choices (account accounts i)
	(store! args (glom "AWSAccountId.member." (1+ i)) account))
      (aws/v4/get #[] (sns-endpoint opts) opts args))))

(defambda (sns/unpermit! topic-arg label (opts base-opts))
  (for-choices topic-arg
    (let* ((arn (sns/topic topic-arg))
	   (args `#["Action" "RemovePermission" "TopicArn" ,arn "Label" ,label]))
      (aws/v4/get #[] (sns-endpoint opts) opts args))))




