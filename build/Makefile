version = $(shell cat ../DESCRIPTION|grep Version:|awk '{print $$2}')
pkg_name = RS3
pkg = $(pkg_name)_$(version).tar.gz

windows_test = ftp://win-builder.r-project.org/


all: prepare_upload

$(pkg):
	R CMD build ../../$(pkg_name)

build: $(pkg)

check: $(pkg)
	R CMD check --as-cran $(pkg)

clean:
	-rm -rf $(pkg) RS3.Rcheck

prepare_upload: clean check
	@echo "Submit $(pkg) $(version) to Windows test:"
	@echo ""
	@echo "curl --disable-epsv -T $(pkg)_$(version) $(cran_ftp)"
	@echo
	@echo ""

.PHONY: build check install clean prepare_upload
