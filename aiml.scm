;;;; -*- mode: scheme; coding: utf-8 -*-

(in-module 'read-aiml)

(module-export! '{read-aiml
                  read-aiml/file})

(use-module '{texttools domutils fdweb})

;;;-------------------------------------------------------------------------------------------------
;;; TODO
;;;-------------------------------------------------------------------------------------------------

;;; - [x] fix read bottlenecks
;;; - [ ] fix DOM read errors
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

;;; Return table values only from XS
(define (tables-only xs)
  (remove-if-not table? xs))

;;; Return a DOM object from argument
(define (xml-objects object)
  (if (string? object)
      (tables-only (xmlparse (flatten-string object)))
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
    (if (> (length parses) 1)
        (error "Too many parses")
        (let ((object (first parses)))
          (if (and (eqv? (get object '%xmltag) 'aiml)
                   (eqv? (get object '%qname) 'aiml))
              #t
              #f)))))

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

;;; Return a matching pattern+template pair from tree
(define (find-pair text tree)
  (let* ((categories (get-categories tree))
         (frame (find-category text categories)))
    `#[,(dom/textify (dom/find frame 'pattern))
       ,(dom/textify (dom/find frame 'template))]))

;;; Search a matching pattern+template pair from file
(define (find-pair/file text file)
  (find-pair text (read-aiml/file file)))

;;; Remove unnecessary characters, punctuations
(define (normalize-pattern text) #f)

;;; Return true if there is only one AIML document in file
(define (single-document? file) #f)

;;; Read an AIML file and return content as entry
(define (read-aiml/file file)
  (let ((val (read-aiml (filestring file))))
    (dom/find val 'aiml)))

;;; Read categories from disk file
(define (get-categories/file file)
  (get-categories (read-aiml/file file)))

;;; Return count of categories from file
(define (count-categories/file file)
  (choice-size (get-categories/file file)))
