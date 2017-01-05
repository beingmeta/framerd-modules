;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'aws/ec2)

(use-module '{logger logctl})
(define %loglevel %notify%)

(use-module '{fdweb texttools mimetable regex 
	      findpath gpath varconfig})
(use-module '{aws aws/v4})

;; Don't issue warnings for these packages when being persnickety
(define %used_modules '{aws varconfig})

(module-export! '{ec2/op ec2/filtered
		  ec2/instances ec2/images 
		  ec2/run! ec2/stop! ec2/terminate!
		  ec2/tags ec2/tag!})

(define action-methods
  #["DescribeInstances" "GET"])

(define (deitemize result (into #[]))
  (do-choices (key (getkeys result))
    (let ((value (get result key)))
      (if (not (table? value))
	  (store! into key value)
	  (if (not (test value 'item))
	      (if (test value '%xmltag key)
		  (let ((copy (deep-copy value)))
		    (drop! copy '{%qname %xmltag})
		    (set! copy (deitemize copy))
		    (unless (empty? (getkeys copy))
		      (store! into key (deitemize copy)))))
	      (do-choices (item (get value 'item))
		(let ((copy (deep-copy item)))
		  (drop! copy '{%qname %xmltag})
		  (set! copy (deitemize copy))
		  (unless (empty? (getkeys copy))
		    (add! into key (deitemize copy)))))))))
  (when (test into '%xmltag (get into '%qname))
    (drop! into '{%xmltag %qname}))
  into)

(define (ec2/op action (args #[]) (opts #f) (req #[]))
  (store! args "Action" action)
  (store! args "Version" "2015-10-01")
  (let* ((method (getopt req 'method
			 (try (get action-methods action)
			      "GET")))
	 (response
	  (aws/v4/op req method "https://ec2.amazonaws.com/" opts
		     args)))
    (reject (elts (xmlparse (getopt response '%content) 'data)) string?)))

(define resource-types
  (downcase
   '{customer-gateway dhcp-options image instance
     internet-gateway network-acl network-interface
     reserved-instances route-table security-group snapshot 
     spot-instances-request subnet volume vpc 
     vpn-connection vpn-gateway}))

(define resource-id-prefixes
  (glom (downcase '{i eipalloc sg eni ami vol snap 
		    subnet vpc rtb igw dopt vpce acl})
    "-"))

(defambda (ec2/filtered action idtypemap (filters '()) (args #[]) (opts #f) (i 1))
  (when (and (odd? (length filters))
	     (table? (car filters)))
    (let ((init (car filters)))
      (do-choices (key (getkeys init))
	(store! args (glom "Filter." i ".Name") 
		(if (symbol? key) (downcase key) key))
	(do-choices (v (get init key) j)
	  (store! args (glom "Filter." i ".Value." (1+ j))
		  (get init key)))
	(set! i (+ i 1)))
      (set! filters (cdr filters))))
  (while (pair? filters)
    (cond  ((not (or (string? (car filters)) (symbol? (car filters))))
	    (irritant (car filters) |Bad filter|))
	   ((overlaps? (downcase (car filters)) resource-types)
	    (store! args (glom "Filter." i ".Name") "resource-type")
	    (do-choices (v (downcase (car filters)) j)
	      (store! args (glom "Filter." i ".Value." (1+ j)) v))
	    (set! filters (cdr filters))
	    (set! i (1+ i)))
	   ((has-prefix (car filters) resource-id-prefixes)
	    (store! args (glom "Filter." i ".Name") "resource-id")
	    (do-choices (v (car filters) j)
	      (store! args (glom "Filter." i ".Value." (1+ j)) v))
	    (set! filters (cdr filters))
	    (set! i (+ i 1)))
	   ((and idtypemap 
		 (test idtypemap (gather #((bos) (isalpha+)) (car filters))))
	    (store! args (glom "Filter." i ".Name") 
		    (get idtypemap (gather #((bos) (isalpha+)) (car filters))))
	    (do-choices (v (car filters) j)
	      (store! args (glom "Filter." i ".Value." (1+ j)) v))
	    (set! filters (cdr filters))
	    (set! i (+ i 1)))
	   ((position #\. (car filters))
	    (store! args (car filters) (cadr filters))
	    (set! filters (cddr filters)))
	   (else
	    (store! args (glom "Filter." i ".Name") 
		    (if (symbol? (car filters)) 
			(downcase (car filters))
			(car filters)))
	    (do-choices (v (cadr filters) j)
	      (store! args (glom "Filter." i ".Value." (1+ j)) v))
	    (set! i (+ i 1))
	    (set! filters (cddr filters)))))
  (ec2/op action args opts))

(define (ec2/instances . filters)
  (let ((response (ec2/filtered 
		   "DescribeInstances"
		   #["i" "instance-id"] filters)))
    (deitemize (get (get (get (get response 'reservationset)
			      'item) 
			 'instancesset)
		    'item))))

(define (ec2/images . filters)
  (let ((response (ec2/filtered "DescribeImages" #["ami" "image-id"] filters)))
    (deitemize (get (get response 'imagesset) 'item))))

;; When searching for a particular tag, use key filter and a value
;; filter, e.g.
;;   (ec2/tags "key" "Name" "value" "worker-1612" )
(define (ec2/tags . filters)
  (let ((response (ec2/filtered "DescribeTags" #f filters)))
    (deitemize (get (get response 'tagset) 'item))))

(defambda (ec2/tag! ids keyvals (opts #f))
  (let ((args #[]) (j 1))
    (do-choices (id ids i)
      ;; Are non-string keys okay here?
      (store! args (glom "ResourceId." (1+ i)) id))
    (do-choices (keyval keyvals)
      (do-choices (key (getkeys keyval))
	(do-choices (value (get keyval key))
	  (store! args (glom "Tag." j ".Key") key)
	  (store! args (glom "Tag." j ".Value") value)
	  (set! j (1+ j)))))
    (ec2/op "CreateTags" args opts)))

;;;; EC2/RUN

(define default-image #f)
(varconfig! ec2:image default-image)

(define default-subnet-id {})
(varconfig! ec2:subnet default-subnet-id)

(define default-vpc #f)
(varconfig! ec2:vpc default-vpc)

(define default-zone "us-east-1e")
(varconfig! ec2:zone default-zone)

(define default-instance-type "t2.micro")
(varconfig! ec2:type default-instance-type)

(define default-profile {})
(varconfig! ec2:profile default-profile)

(define (ec2/run! args (req #[]) (opts #f))
  (let* ((call
	  (frame-create #f
	    "DryRun" (getopt args 'dryrun {})
	    "EbsOptimized" (getopt args 'ebsopt #f)
	    "DisableApiTermination" (getopt args 'highlander {})
	    "IamInstanceProfile" (lookup-profile (getopt args 'profile "*"))
	    "ImageId" (try (pick (getopt args 'ami {}) has-prefix "ami-")
			   (lookup-image (getopt args 'ami #f)))
	    "InstanceType" (getopt args 'type default-instance-type)
	    "KeyName" (getopt args 'keyname {})
	    "Monitoring" (getopt args 'monitoring {})
	    "SecurityGroupId*" (pick (getopt args 'security {}) has-prefix "sg-")
	    "SecurityGroup*" (reject (getopt args 'security {}) has-prefix "sg-")
	    "MinCount" (getopt args 'count 1)
	    "MaxCount" (getopt args 'maxcount (getopt args 'count 1))
	    "ClientToken" (getopt args 'requestid {})
	    "Placement"
	    (join-commas
	     {(glom "AvailabilityZone=" (getopt args 'zone default-zone))
	      (glom "GroupName=" (getopt args 'group {}))
	      (glom "Affinity=" (getopt args 'affinity {}))
	      (glom "HostId=" (getopt args 'hostid {}))
	      (glom "Tenancy=" (getopt args 'tenancy {}))})
	    "SubnetId" (get-subnet-id args)
	    "UserData" (encode-user-data (getopt args 'userdata {}))))
	 (instance (ec2/op "RunInstances" call opts))
	 (ids (find-paths instance 'instanceid)))
    (loginfo |GotInstances| ids)
    (when (testopt args 'tags) 
      (ec2/tag! ids (fix-tags (getopt args 'tags))))
    instance))

(defambda (join-commas clauses)
  (stringout (do-choices (clause clauses i)
	       (if (> i 0) (printout ","))
	       (if (string? clause) (printout clause)))))

(define (fix-tags tags)
  (let ((result (frame-create #f)))
    (do-choices (key (getkeys tags))
      (add! result 
	    ;; Handle the name field especially since it has
	    ;;  special interpretation by the AWS console
	    (if (equal? (downcase key) "name") "Name"
		(if (string? key) key
		    (if (symbol? key)
			(if (uppercase? (symbol->string key))
			    (downcase key)
			    (symbol->string key))
			key)))
	    (get tags key)))
    result))

(define (get-default-vpc args)
  (let ((vpcs (pick (get (find-path (ec2/op "DescribeVpcs") 'vpcset) 'items)
		    'state "available")))
    (if (singleton? vpcs)
	(get vpcs 'vpcid)
	(try (get (pick vpcs 'isdefault "true") 'vpcid)
	     (get (pick-one vpcs) 'vpcid)
	     default-vpc))))

(define (get-subnet-id args)
  (getopt args 'subnet 
	  (let ((subnets (find-path (ec2/op "DescribeSubnets") 'subnetset))
		(zone (getopt args 'zone default-zone))
		(vpc (getopt args 'vpc (get-default-vpc args))))
	    (try
	     (tryif vpc
	       (get (pick (get subnets 'item) 'availabilityzone zone 'vpcid vpc) 'subnetid))
	     (get (pick (get subnets 'item) 'availabilityzone zone) 'subnetid)
	     default-subnet-id))))

(define (encode-user-data s)
  (if (string? s)
      (if (has-prefix s "@")
	  (->base64 (gp/fetch (slice s 1)))
	  (->base64 s))
      (if (packet? s) (->base64 s)
	  (irritant s |BadUserData| "Not yet supported"))))

(define (lookup-profile name)
  (if (equal? name "*") default-profile
      (if (and (has-prefix name "arn:aws:iam:")
	       (search ":instance-profile/" name))
	  name
	  (try (iam-lookup-profile name)
	       (irritant name |UnknownRole| lookup-role)))))
(define (lookup-image image)
  (if (equal? image "*") 
      (or default-image (error |NoDefaultImage| "For EC2"))
      (get (try (ec2/images "name" image)
		(ec2/images "tag:Name" image)
		(error |UnknownImage| image))
	   'imageid)))

(define (iam-lookup-profile name)
  (let*  ((result (aws/v4/op `#[] "GET" "https://iam.amazonaws.com/"
			     `#[accept "application/json"]
			     `#["Action" "GetInstanceProfile"
				"Version" "2010-05-08"
				"InstanceProfileName" ,name]))
	  (response (car result))
	  (parsed (and (response/ok? response)
		       (onerror
			   (jsonparse (get response '%content))
			 (lambda (ex)
			   #f))))
	  (arn (and parsed 
		    (try (find-path parsed 'instanceprofile 'arn)
			 #f))))
    (or arn
	(irritant name |CannotResolveInstanceProfile|
		  iam-lookup-profile))))


;;;; Stop instances

(define (ec2/stop! instances (args #f))
  (let* ((call
	  (frame-create #f
	    "DryRun" (getopt args 'dryrun {})
	    "Force" (getopt args 'force {})
	    "InstanceId*" (choice (pickstrings instances)
				  (find-paths (pick instances table?) 'instanceid))))
	 (result (ec2/op "StopInstances" call)))
    result))

(define (ec2/terminate! instances (args #f))
  (let* ((call
	  (frame-create #f
	    "DryRun" (getopt args 'dryrun {})
	    "Force" (getopt args 'force {})
	    "InstanceId*" (choice (pickstrings instances)
				  (find-paths (pick instances table?) 'instanceid))))
	 (result (ec2/op "TerminateInstances" call)))
    result))




