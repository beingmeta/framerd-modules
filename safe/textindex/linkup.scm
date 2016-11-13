;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; This does simple keytuple extraction from (currently) English
;;;  text, using a handful of simple rules.

(in-module 'textindex/linkup)

(use-module '{texttools tagger})

;;; Keytuple extraction

(define (get-keytuples parse)
  (if (string? parse)
      (get-keytuples (tagtext parse))
      (choice (getxkeys parse
			'{noun plural-noun solitary-noun proper-name}
			'{preposition adjective count-adjective proper-modifier
				      possessive proper-possessive}
			'{preposition})
	      (getxkeys parse
			'{noun noun-modifier}
			'{adjective possessive proper-possessive proper-modifier}
			'{})
	      (getxkeys parse
			'{adjective count-adjective}
			'{adverb}
			'{})
	      (getxkeys parse
			'{verb complement-adjective be-verb
			       ing-verb inflected-verb infinitival-verb}
			'{adverb negator modal-aux be-aux
				 noun plural-noun solitary-noun
				 proper-name}
			'{noun plural-noun solitary-noun preposition
			       trailing-adverb negator
			       proper-name}))))
(define (get-keylinks parse)
  (if (string? parse)
      (get-keylinks (tagtext parse))
      (choice (getxlinks parse
			 '{noun plural-noun solitary-noun proper-name}
			 '{preposition adjective count-adjective proper-modifier
			   possessive proper-possessive}
			 '{})
	      (getxlinks parse
			 '{noun noun-modifier}
			 '{adjective possessive proper-possessive proper-modifier}
			 '{})
	      (getxlinks parse
			 '{adjective count-adjective}
			 '{adverb}
			 '{})
	      (getxlinks parse
			 '{verb complement-adjective be-verb
			   ing-verb inflected-verb}
			 '{adverb negator modal-aux be-aux
			   noun plural-noun solitary-noun
			   proper-name}
			 '{noun plural-noun solitary-noun preposition
			   trailing-adverb negator
			   proper-name}))))

(define linkup-rules
  '{
    #(HEAD {noun plural-noun solitary-noun proper-name}
      {preposition adjective count-adjective proper-modifier
       possessive proper-possessive determiner singular-determiner 
       verb complement-adjective be-verb}
      {}
      {noun plural-noun solitary-noun proper-name
       ing-verb inflected-verb infinitival-verb})
    #(HEAD {noun noun-modifier}
      {adjective possessive proper-possessive proper-modifier}
      {}
      {noun plural-noun solitary-noun proper-name
       ing-verb inflected-verb infinitival-verb
       singular-determiner})
    #(HEAD {adjective count-adjective}
      {adverb}
      {})
    #(HEAD
      {verb complement-adjective be-verb
       ing-verb inflected-verb infinitival-verb}
      {adverb negator modal-aux be-aux
       noun plural-noun solitary-noun
       proper-name}
      {noun plural-noun solitary-noun preposition
       trailing-adverb negator
       proper-name})})

(define (linkup parse)
  (if (string? parse)
      (taglink (tagtext parse) linkup-rules)
      (taglink parse linkup-rules)))

(module-export! '{get-keytuples get-keylinks linkup})
