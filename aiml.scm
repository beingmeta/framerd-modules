;;;; -*- mode: scheme; coding: utf-8 -*-

(in-module 'read-aiml)

(module-export! '{read-aiml
                  read-aiml-file
                  ->aiml
                  ->aiml/first
                  ->aiml/last
                  aiml->choices})

(use-module '{texttools domutils fdweb})


;;; TODO
;;; - [x] return choices of questions and answers
;;; - [x] return list of questions and answers
;;; - [ ] handle <star/>
;;; - [ ] handle <srai>
;;; - [ ] perform resolution of references
;;; - [ ] match a question with an answer
;;; - [ ] extract specific question and answer pairs
;;; - [ ] read-aiml-file: ensure that only one AIML instance appears per document
;;; - [ ] read-aiml-file: ensure correctness


;;;-------------------------------------------------------------------------------------------------
;;; List, map, and friends
;;;-------------------------------------------------------------------------------------------------

;;; Return argument V
(define (identity v) v)

;;; Return list argument as choices
(define (list->choice xs)
  (map->choice identity xs))

;;; TODO: write nondeterministic versions

;;; Return true if PROC returns true for all XS
(define (andmap proc xs)
  (if (memq #f (map proc xs))
      #f
      #t))

;;; Return true if PROC returns at least one true for all XS
(define (ormap proc xs)
  (if (memq #t (map proc xs))
      #t
      #f))

(define every andmap)
(define some ormap)

;;; Return last object from XS
(define (last xs)
  (first (reverse xs)))


;;;-------------------------------------------------------------------------------------------------
;;; Basics
;;;-------------------------------------------------------------------------------------------------

;;; Flatten string, removing newlines
(define (flatten-string str)
  (remove #\newline str))

;;; Return a DOM object from argument
(define (ensure-xml-object object)
  (if (string? object)
      (xmlparse (flatten-string object))
      object))

;;; Return an AIML object from object
(define (->aiml object)
  (if (string? object)
      (let ((parse (ensure-xml-object object)))
        (if (and (list? parse)
                 (not (zero? (length parse))))
            parse))))

;;; Return first AIML object from XS
(define (->aiml/first xs)
  (first (->aiml xs)))

;;; Return last AIML object from XS
(define (->aiml/last xs)
  (last (->aiml xs)))

;;; Return true if object is indeed an AIML object
(define (aiml-object? object)
  (let ((parses (ensure-xml-object object)))
    (if (> (length parses) 1)
        (error "Too many parses")
        (let ((object (first parses)))
          (if (and (eqv? (get object '%xmltag) 'aiml)
                   (eqv? (get object '%qname) 'aiml))
              #t
              #f)))))

;;; Return true if all objects in XS are AIML objects
(define (aiml-objects? . objects)
  (andmap aiml-object? objects))

;;; Return AIML document version
(define (aiml-version object)
  (dom/get object 'version))

;;; Return AIML document encoding
(define (aiml-encoding object)
  (dom/get object 'encoding))

;;; (->aiml/first "<aiml version=\"1.0.1\" encoding=\"UTF-8\"?></aiml>")

;;; (->aiml/first "<aiml version=\"1.0.1\" encoding=\"UTF-8\"?><category><pattern>HELLO BOT!</pattern><template>Hello my new friend!</template></category></aiml>")

(define tree1
  "<aiml version=\"1.0.1\" encoding=\"UTF-8\"?>
   <category>
      <pattern> HELLO BOT! </pattern>
      <template> Hello my new friend! </template>
   </category>
   <category>
     <pattern> I LIKE * </pattern>
     <template>I like <star/>, too.</template>
   </category>
</aiml>")

(define ptree1 (->aiml/first tree1))
(define qtree1 (list->choice (->aiml tree1)))

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
;;; (find-category "HELLO BOT!" (get-categories ptree1))
(define (find-category text categories)
  (filter-choices (cat categories)
    (let ((pattern (dom/find cat 'pattern)))
      (textmatch `#((isspace+) ,text (isspace+))
                 (dom/textify pattern)))))

;;; Return a 
;;; (dom/textify (dom/find (find-category "HELLO BOT!" (get-categories ptree1)) 'pattern))
(define (find-pattern-template-pair text categories)
  (let ((frame (find-category text categories)))
    ;; `#[,(dom/find frame 'pattern)
    ;;    ,(dom/find frame 'template)]
    `#[,(dom/textify (dom/find frame 'pattern))
       ,(dom/textify (dom/find frame 'template))]))

;;; Return <pattern> <template> pairs as choice of tables
(define (get-pattern-template-pairs tree)
  (for-choices (cat (get-categories tree))
    `#[,(dom/textify (dom/find cat 'pattern))
       ,(dom/textify (dom/find cat 'template))]))

;;; Read an AIML file and return content as entries
(define (read-aiml-file file)
  (->aiml (filestring file)))

;;; Return true if there is only one AIML document in file
(define (single-document? file) #f)

;;; Return an AIML object as choices
(define (aiml->choices object) #f)
