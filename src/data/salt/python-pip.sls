python-pip:
  pkg.installed:
    - pkgs:
      - python3-pip

install_pipenv:
  pip3.installed:
    - name: pipenv
    - upgrade: True
  
install_dvc:
  pip3.installed:
    - name: dvc
    - upgrade: False