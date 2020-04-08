python-pip:
  pkg.installed:
    - pkgs:
      - python3-pip

install_pipenv:
  pip.installed:
    - name: pipenv
    - upgrade: True