SCHEME_FILES = $(shell find . -name "*.scm")
LOCAL_MODULES = $(shell fdconfig local_modules)
LOCAL_SAFE_MODULES = $(shell fdconfig local_safe_modules)
ROOT_DIR = $(shell pwd)

TAGS: $(SCHEME_FILES)
	etags *.scm
	find . -name "*.scm" -exec etags -o TAGS -a {} \;

optall:
	fdexec optall.scm
test: optall

link:
	for x in $(ROOT_DIR)/*.scm; do ln -sf $$x $(LOCAL_MODULES); done
	for x in $(ROOT_DIR)/*/module.scm; do ln -sf `dirname $$x` $(LOCAL_MODULES); done
	for x in $(ROOT_DIR)/safe/*.scm;  do ln -sf $$x $(LOCAL_SAFE_MODULES); done
	for x in $(ROOT_DIR)/safe/*/module.scm; do ln -sf `dirname $$x` $(LOCAL_SAFE_MODULES); done

freshtags:
	rm TAGS
	make TAGS
