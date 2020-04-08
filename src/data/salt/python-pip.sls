python-pip:
  pkg.installed:
    - pkgs:
      - python3-pip

install_pipenv:
  pip.installed:
    - name: pipenv
    - upgrade: True
    - pip_bin: pip3
  
install_dvc:
  pip.installed:
    - name: dvc
    - upgrade: False
    - pip_bin: pip3