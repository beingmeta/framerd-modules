;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'brico/wordnet)

(use-module '{texttools})
(use-module '{logger varconfig optimize stringfmts})
(use-module '{storage/flex})
(use-module '{brico})

(module-export! '{link-release! check-release-links
		  import-synsets read-synset})

(define wordnet-source @1/46074)
(define en @1/2c1c7)

(define wnrelease 'wn30)
(varconfig! brico:wnversion wnrelease)

(define sensecats
  #(ADJ.ALL ADJ.PERT ADV.ALL NOUN.TOPS NOUN.ACT NOUN.ANIMAL NOUN.ARTIFACT 
    NOUN.ATTRIBUTE NOUN.BODY NOUN.COGNITION NOUN.COMMUNICATION NOUN.EVENT 
    NOUN.FEELING NOUN.FOOD NOUN.GROUP NOUN.LOCATION NOUN.MOTIVE NOUN.OBJECT 
    NOUN.PERSON NOUN.PHENOMENON NOUN.PLANT NOUN.POSSESSION NOUN.PROCESS 
    NOUN.QUANTITY NOUN.RELATION NOUN.SHAPE NOUN.STATE NOUN.SUBSTANCE NOUN.TIME 
    VERB.BODY VERB.CHANGE VERB.COGNITION VERB.COMMUNICATION VERB.COMPETITION 
    VERB.CONSUMPTION VERB.CONTACT VERB.CREATION VERB.EMOTION VERB.MOTION 
    VERB.PERCEPTION VERB.POSSESSION VERB.SOCIAL VERB.STATIVE VERB.WEATHER ADJ.PPL))

(define core.index #f)
(define wordnet.index (open-index (mkpath brico-dir "wordnet.index")))
(define wordforms.index (open-index (mkpath brico-dir "wordforms.index")))

(set! core.index (open-index (mkpath brico-dir "core.index")))
(set! wordnet.index (open-index (mkpath brico-dir "wordnet.index")))
(set! wordforms.index (open-index (mkpath brico-dir "wordforms.index")))
(poolctl brico.pool 'readonly #f)
(indexctl core.index 'readonly #f)
(indexctl wordnet.index 'readonly #f)
(indexctl wordforms.index 'readonly #f)

;;; Reading WordNet sense indexes (index.sense)

(define (sense-index-reader in (line))
  (set! line (getline in))
  (and (exists? line) (string? line)
       (let ((s (segment line " ")))
	 `#[key ,(first s) 
	    synset ,(string->number (second s)) 
	    sense_no ,(string->number (third s))
	    freq ,(string->number (fourth s))
	    line ,line])))

(define (read-sense-index in)
  (when (string? in) (set! in (open-input-file in)))
  (let ((results {})
	(sense (sense-index-reader in))
	(count 1))
    (while sense
      (set+! results sense)
      (set! count (1+ count))
      (set! sense (sense-index-reader in)))
    results))

(define (link-release! sense-index (release wnrelease))
  (let ((senses (read-sense-index sense-index))
	(wnoids (find-frames wordnet.index 'has 'sensekeys))
	(synsetmap (make-hashtable))
	(sensemap (make-hashtable)))
    (prefetch-keys! wordnet.index (cons 'sensekeys (get senses 'key)))
    (lock-oids! wnoids)
    (prefetch-oids! wnoids)
    (do-choices (sense senses)
      (add! sensemap (get sense 'key) sense)
      (let* ((oid (find-frames wordnet.index 'sensekeys (get sense 'key)))
	     (type (intersection (get oid 'type) '{noun verb adjective adverb}))
	     (synset-key (list release (get sense 'synset) type)))
	(when (exists? oid)
	  (add! oid '%synsets synset-key)
	  (add! synsetmap oid synset-key))
	(index-frame wordnet.index oid '%synsets)
	(index-frame wordnet.index oid 'type)
	(index-frame wordnet.index oid 'has (getkeys oid))))
    `#[sensemap ,sensemap synsetmap ,synsetmap]))

(define (check-release-links wnrelease)
  "This returns any cases where the synset mapping for WNRELEASE is ambiguous"
  (let ((tagged (find-frames wordnet.index 'has '{sensekeys %synsets})))
    (prefetch-oids! tagged)
    (filter-choices (synset tagged)
      (ambiguous? (pick (get synset '%synsets) wnrelease)))))

;;;; Reading WordNet synset data files (data.pos)

(define typecodes
  #["n" noun "v" verb "a" adjective "s" {adjective_satellite adjective} "r" adverb])
(define ptrcodes
  #[noun
    #["!" antonym "@" hypernym "@i" isa "!" hyponym "~i" instances
      "#m" memberof "#s" ingredientof "#p" partof
      "%m" members "%s" ingredients "%p" parts
      "=" attribute "+" derivations
      ";c" topic "-c" topicof
      ";r" region "-r" regionof
      ";u" usage "-u" usage]
    verb
    #["!" antonym "@" hypernym "~" hyponym "*" entails ">" causes
      "^" seealso "$" verbgroup "+" derivations
      ";c" topic ";r" region ";u" usage]
    adjective
    #["!" antonym "&" similar "<" participle "\\" pertainym 
      "=" attribute "^" seealso ";c" topic ";r" region ";u" usage]
    adverb
    #["!" antonym "\\" adjective ";c" topic ";r" region ";u" usage]])

(define (decnum string) (string->number string 10))
(define (hexnum string) (string->number string 16))

(define (line->synset line)
  (let* ((glosspos (position #\| line))
	 (vec (->vector (textslice (slice line 0 glosspos) '(isspace+) #f)))
	 (n-words (hexnum (elt vec 3)))
	 (wordvec (slice vec 4 (+ 4 (* n-words 2))))
	 (type (try (get typecodes (elt vec 2)) (elt vec 2)))
	 (ptrmap (get ptrcodes type))
	 (wordforms (make-vector n-words)))
    (dotimes (i n-words)
      (vector-set! wordforms i 
		   (cons (string-subst (elt wordvec (* 2 i)) "_" " ")
			 (hexnum (elt wordvec (1+ (* 2 i)))))))
    (let ((info
	   (frame-create #f
	     'synset (decnum (elt vec 0))
	     'fileno (decnum (elt vec 1))
	     'sensecat (elt sensecats (decnum (elt vec 1)))
	     'type type 'n-words n-words
	     'wordforms wordforms 'words (car (elts wordforms))
	     'gloss (tryif glosspos (stdspace (slice line (1+ glosspos))))))
	  (n-ptrs (decnum (elt vec (+ 4 (* n-words 2)))))
	  (ptroff (+ 5 (* n-words 2))))
      (dotimes (i n-ptrs)
	(let* ((ptr (slice vec (+ ptroff (* i 4)) (+ 4 ptroff (* i 4))))
	       (relation (get ptrmap (elt ptr 0)))
	       (target (decnum (elt ptr 1)))
	       (target-pos (try (get typecodes (elt ptr 2)) (elt ptr 2)))
	       (lexinfo (elt ptr 3))
	       (source-wordno (hexnum (slice lexinfo 0 2)))
	       (target-wordno (hexnum (slice lexinfo 2))))
	  (add! info 'pointers 
		(frame-create #f
		  'relation relation 'type target-pos 'synset target
		  'source (tryif (> source-wordno 0) (car (elt wordforms (1- source-wordno))))
		  'sourceno (tryif (> source-wordno 0) source-wordno)
		  'targetno (tryif (> target-wordno 0) target-wordno)))))
      (when (> (length vec) (+ ptroff (* 4 n-ptrs)))
	(let* ((n-frames (decnum (elt vec (+ ptroff (* 4 n-ptrs)))))
	       (verb-frames (slice vec (+ 1 ptroff (* 4 n-ptrs)))))
	  (dotimes (i n-frames)
	    (let* ((fnum (decnum (elt verb-frames (+ 1 (* i 3)))))
		   (wnum (hexnum (elt verb-frames (+ 2 (* i 3)))))
		   (vframe (try (find-frames wordnet.index 'vframenum fnum) fnum)))
	      (if (zero? wnum)
		  (add! info 'vframes vframe)
		  (add! info 'vframes 
			(cons (elt wordforms (-1+ wnum)) vframe)))))))
      info)))

;;;; Mapping synset descriptions into existing BRICO concepts

(define (ref-synset num type (version wnrelease))
  (let* ((frame (find-frames wordnet.index
		  '%synsets `(,version ,num ,type))))
    (when (fail? frame)
      (set! frame 
	(frame-create brico.pool
	  'type type '%synsets `(,version ,num ,type)))
      (lognotice |NewSynsetRef| type " " num)
      (index-frame {wordnet.index core.index} frame 'type type)
      (index-frame wordnet.index frame '%synsets))
    frame))

(define (import-synset info)
  (let* ((existing (find-frames wordnet.index
		     '%synsets `(,wnrelease ,(get info 'synset) ,(get info 'type))))
	 (frame (try existing
		     (frame-create brico.pool
		       'type (get info 'type)
		       '%synsets `(,wnrelease ,(get info 'synset) ,(get info 'type))))))
    (store! frame '%id
	    (cons (try (get info 'sensecat) wnrelease)
		  (choice->list (get info 'words))))
    (add! frame 'source wordnet-source)
    (drop! frame '%words (cons 'en (get frame 'words)))
    (store! frame 'words (get info 'words))
    (add! frame '%words (cons 'en (get info 'words)))
    (drop! frame '%norms (pick (get frame '%norms) en))
    (drop! frame '%glosses (cons en (get frame 'gloss)))
    (store! frame 'gloss (get info 'gloss))
    (add! frame '%glosses (cons @?en (get info 'gloss)))
    (store! frame 'sensecat (get info 'sensecat))
    (store! frame 'ranked (map car (get info 'wordforms)))
    (store! frame 'n-words (get info 'n-words))
    (drop! frame (get (get info 'pointers) 'relation))
    (drop! frame '{hypernym hyponym})
    (do-choices (ptr (get info 'pointers))
      (let* ((relation (get ptr 'relation))
	     (synset-target (ref-synset (get ptr 'synset) (get ptr 'type)))
	     (target (try (get-wordform synset-target (get ptr 'targetno) #f
					(cons frame relation))
			  synset-target))
	     (source (try (get-wordform frame (get ptr 'sourceno) (get ptr 'source)
					(cons relation synset-target))
			  frame)))
	(add! source relation target)))
    (store! frame '%forms
	    (forseq (form (get info 'wordforms) i)
	      (when (zero? (cdr form)) 
		(add! frame 'norms (car form))
		(add! frame '%norms (cons en (car form))))
	      (let ((cur (?? 'word (car form) 'of existing)))
		(cond ((fail? cur))
		      ((test cur 'sensenum (1+ i)))
		      ((test cur 'sensenum)
		       (warn%watch "SenseNumMismatch"
			 frame form "WORDFORMS" (get info 'wordforms)
			 "SYNSETS" (get frame '%synsets))
		       (set! cur {}))
		      (else (store! cur 'word (car form))
			    (index-frame wordforms.index cur 'word (car form))))
		(try cur form))))
    (when (fail? existing)
      (lognotice |NewSynset| frame)
      (index-frame core.index frame 'type))))

(define (fix-wordform wf)
  (unless (test wf 'sensenum)
    (store! wf 'sensenum
	    (or (position (get wf 'word) (get (get wf 'of) 'ranked)) {}))
    (index-frame wordforms.index wf 'sensenum)))

(define (get-wordform meaning sensenum (word #f) (context #f))
  (let ((existing 
	 (try (tryif word
		(find-frames wordforms.index 
		  'of meaning 'word word))
	      (find-frames wordforms.index 
		'of meaning 'sensenum sensenum))))
    (cond ((fail? existing)
	   (let ((form (frame-create brico.pool
			 'type 'wordform 'of meaning
			 'word (tryif word word)
			 'sensenum sensenum)))
	     (index-frame wordforms.index form '{type word sensenum of})
	     form))
	  ((not (test existing 'sensenum sensenum))
	   (when (test existing 'sensenum)
	     (warn%watch "SenseNumChanged"
	       meaning sensenum word existing 
	       "OLD" (get existing 'sensenum)
	       "SYNSETS" (get meaning '%synsets)
	       "CONTEXT" context)
	     (drop! wordforms.index (cons 'sensenum (get existing 'sensenum))
		    existing))
	   (store! existing 'sensenum sensenum)
	   (index-frame wordforms.index existing 'sensenum sensenum)
	   existing)
	  (else existing))))

;;;; Reading synset data files

(define (dataline in (line))
  (default! line (getline in))
  (if (fail? line) #eof
      (if (has-prefix line " ")
	  (dataline in)
	  line)))

(define (read-synset file off)
  (let ((in (open-input-file file)))
    (setpos! in off)
    (line->synset (getline in))))

(define (import-synsets file)
  (let* ((in (open-input-file file))
	 (line (dataline in)))
    (while (and line (not (eq? line #eof)))
      (import-synset (line->synset line))
      (set! line (dataline in)))))


;;;; The default order

(comment
 (link-release! "dict/index.sense" 'wn30)
 (when (exists? (check-release-links 'wn30))
   (error |InconsistentLinks|))
 (import-synsets "dict/data.noun")
 (import-synsets "dict/data.verb")
 (import-synsets "dict/data.adj")
 (import-synsets "dict/data.adv")
 (fix-wordform (difference
		(find-frames wordnet.index 
		  'type 'wordform)
		(find-frames wordnet.index 
		  'has 'sensenum))))

;;; Various old code used to set up the database

;;; How verb frames were registered
#|
(doseq (line (remove "" (segment (filestring "vframes.text") "\n")))
  (let* ((pos (position #\Space line))
	 (num (string->number (trim-spaces (slice line 0 pos))))
	 (text (trim-spaces (slice line pos)))
	 (vframe (find-frames wordnet.index 'text text)))
    (if (fail? vframe) (logwarn |NoMap| text)
	(begin (store! vframe 'vframenum num) 
	  (index-frame wordnet.index vframe 'vframenum)))))
|#

;;; This is the code which was used to fix the sense-keys based on the
;;; wn1.6 index.sense file.
#|
(define (sense->synset sense) (find-frames {wordnet.index sensekeys.index} '%wn16 (get sense 'synset) 'sense-keys (get sense 'key)))
(define sensekeys.table (make-hashtable))
(define wn16.senses (read-sense-index "wordnet/WordNet-1.6/dict/index.sense"))
(do-choices (sense wn16.senses)
  (let ((synset (sense->synset sense)))
    (if (ambiguous? synset)
	(logwarn |AmbigSynset| sense " ==> " synset)
	(add! sensekey.map synset (get sense 'key)))))
(do-choices (key (getkeys sensekeys.table))
  (store! key 'sensekeys (get sensekeys.table key))
  (index-frame wordnet.index key 'sensekeys))
|#

;;; Creating a sensekey index when you don't have one
#|
(define sense-keys.index
  (db/ref (mkpath brico-dir "sensekeys.index")
	  #[indextype hashindex size #2mib 
	    keyslot sensekeys
	    register #t create #t]))

(define (index-sense-keys)
  (do-choices (f (?? 'has 'sense-keys))
    (add! sense-keys.index (cdr (get f 'sense-keys)) f)))

(unless (exists? (find-frames sense-keys.index 'sense-keys "flutter%1:04:00::"))
  (logwarn |IndexingSenseKeys| "Generating sense-key index")
  (optimize! index-sense-keys)
  (index-sense-keys))
|#

