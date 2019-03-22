;;;; -*- mode: scheme; coding: utf-8 -*-

(in-module 'read-aiml)

(module-export! '{read-aiml
                  read-aiml/file})

(use-module '{texttools domutils fdweb})

;;;-------------------------------------------------------------------------------------------------
;;; TODO
;;;-------------------------------------------------------------------------------------------------

;;; - [x] handle <random>
;;; - [ ] handle <star/>
;;; - [ ] handle <srai>


;;;-------------------------------------------------------------------------------------------------
;;; List, map, and friends
;;;-------------------------------------------------------------------------------------------------

;;; Return argument V
(define (identity v) v)

;;; Return list argument as choices
(define (list->choice xs)
  (map->choice identity xs))

;;; Return last object from XS
(define (last xs)
  (first (reverse xs)))


;;;-------------------------------------------------------------------------------------------------
;;; Basics
;;;-------------------------------------------------------------------------------------------------

;;; Flatten string, removing newlines
(define (flatten-string str)
  (textsubst str "\n" ""))

;;; Return %xmltag value of object
(define (xmltag object)
  (get object '%xmltag))

;;; Return %qname value of object
(define (qname object)
  (get object '%qname))

;;; Return true if tag is a comment
(define (comment-object? object)
  (eqv? (xmltag object) '%comment))

;;; Return true if tag is AIML
(define (aiml-object? object)
  (and (eqv? (xmltag object) 'aiml)
       (eqv? (qname object) 'aiml)))

;;; Return table values only from XS
(define (tables-only xs)
  (remove-if-not table? xs))

;;; Remove comment objects from objects
(define (remove-comments xs)
  (remove-if comment-object? xs))

;;; Return a DOM object from argument
(define (xml-objects object)
  (if (string? object)
      (let ((val (xmlparse (flatten-string object))))
        (remove-comments (tables-only val)))
      ;; (tables-only (xmlparse (flatten-string object)))
      object))

;;; Return multiple AIML objects as choices
(define (read-aiml/choices object)
  (and (string? object)
       (list->choice (xml-objects object))))

;;; Return a single AIML object from choices
(define (read-aiml/one object)
  (let ((val (read-aiml/choices object)))
    (if (> (choice-size val) 1)
        (error "Too many parses")
        (pick-one val))))

;;; Return an AIML object from object
(define read-aiml read-aiml/one)

;;; Return true if object is indeed an AIML object
(define (aiml-object? object)
  (let ((parses (xml-objects object)))
    (if (and (eqv? (get object '%xmltag) 'aiml)
             (eqv? (get object '%qname) 'aiml))
        #t
        #f)))

;;; Return true if all objects in XS are AIML objects
(define (aiml-objects? . objects)
  (every? aiml-object? objects))

;;; Return AIML document version
(define (aiml-version object)
  (dom/get object 'version))

;;; Return AIML document encoding
(define (aiml-encoding object)
  (dom/get object 'encoding))

;;; Return attributes of element
(define (aiml-attributes object) #f)

;;; Return all <category> objects from tree
(define (get-categories tree)
  (dom/find tree 'category))

;;; Return all <pattern> objects from tree
(define (get-patterns tree)
  (for-choices (category (get-categories tree))
    (identity (dom/find category 'pattern))))

;;; Return all <pattern> object texts
(define (get-pattern-texts tree)
  (for-choices (pattern (get-patterns tree))
    (dom/textify pattern)))

;;; Return all <template> objects from tree
(define (get-templates tree)
  (for-choices (category (get-categories tree))
    (identity (dom/find category 'template))))

;;; Return all <templaten> object texts
(define (get-template-texts tree)
  (for-choices (template (get-templates tree))
    (dom/textify template)))

;;; Return a category object in which a pattern is found
(define (find-category text categories)
  (filter-choices (cat categories)
    (let* ((pattern (dom/find cat 'pattern))
           (closure {#((isspace+) (ic text) (isspace+))
                     #((isspace+) (ic text))
                     #((ic text) (isspace+))
                     #((ic text))}))
      ;; (textmatch (textclosure '#(closure)) (dom/textify pattern))
      (textsearch `(ignore-case ,text) (dom/textify pattern)))))

;;; Return a random pair of pattern and template from file
(define (random-pair/file file)
  (pick-one (get-pairs/file file)))

;;; Test
;; (define entry (find-category "four cultures" (get-categories (read-aiml/file "~/Downloads/dat/aiml/aiml/aiml/botdata/alice/stories.aiml"))))

;;; Return the random element if it exists under entry
(define (get-random entry)
  (let ((val (dom/find (dom/find entry 'template) 'random)))
    (if (zero? (choice-size val))
        #f
        val)))

;;; Return true if a category has a random element under its pattern
(define (has-random? entry)
  (if (get-random entry)
      #t
      #f))

;;; Return list entries of a random element
(define (random-entries entry)
  (tables-only (dom/get (get-random entry) '%content)))

;;; Return a random entry
(define (random-entry entries)
  (let* ((len (length entries))
         (rand (random len)))
    (elt entries rand)))

;;; Return text from random entry
(define (random-entry/text entry)
  (textsubst (dom/textify (random-entry (random-entries entry)))
             "\n" ""))

;;; Dispatch an operation based on the type of template
(define (dispatch-entry entry)
  #f)

;;; Return a simple text value from entry
(define (entry/text entry type)
  (dom/textify (dom/find entry type)))

;;; Return a frame of pattern and template from entry
;;; Conditionally dispatch values based on type
(define (entry-texts entry)
  `#[,(dom/textify (dom/find entry 'pattern))
     ,(dom/textify (dom/find entry 'template))])

(define (entry-texts entry)
  (let ((key (dom/textify (dom/find entry 'pattern)))
        (val (cond ((has-random? entry) (random-entry/text entry))
                   (else (entry/text entry 'template)))))
    `#[,key ,val]))

;;; Return a matching pattern+template pair from tree
(define (find-pair text tree)
  (let* ((categories (get-categories tree))
         (frame (find-category text categories)))
    (entry-texts frame)))

;;; Search a matching pattern+template pair from file
(define (find-pair/file text file)
  (find-pair text (read-aiml/file file)))

;;; Read an AIML file and return content as entry
(define (read-aiml/file file)
  (let ((val (read-aiml (filestring file))))
    (dom/find val 'aiml)))

;;; Read categories from file
(define (get-categories/file file)
  (get-categories (read-aiml/file file)))

;;; Return a random category from file
(define (random-category/file file) #f)

;;; Return count of categories from file
(define (count-categories/file file)
  (choice-size (get-categories/file file)))

;;; Return all pairs of pattern and templates from file
(define (get-pairs/file file)
  (let ((categories (get-categories/file file)))
    (for-choices (entry categories)
      (entry-texts entry))))