version = $(shell cat ../DESCRIPTION|grep Version:|awk '{print $$2}')
pkg_name = RS3
pkg = $(pkg_name)_$(version).tar.gz

windows_test_ftp = ftp://win-builder.r-project.org/R-release/


all: prepare_upload

$(pkg):
	R CMD build ../../$(pkg_name)

build: $(pkg)

check: $(pkg)
	R CMD check $(pkg)

clean:
	-rm -rf $(pkg) RS3.Rcheck

prepare_upload: clean check
	@echo "Submit $(pkg) $(version) to Windows test:"
	@echo ""
	@echo "curl --disable-epsv -T $(pkg) $(windows_test_ftp)"
	@echo
	@echo ""
	@echo "uploading file now"
	@curl --disable-epsv --ftp-skip-pasv-ip --data-binary $(pkg)  $(windows_test_ftp)
	cp $(pkg) /home/evan/Downloads/te/

.PHONY: build check install clean prepare_upload
