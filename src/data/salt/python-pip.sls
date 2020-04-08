python-pip:
  pkg.installed:
    - pkgs:
      - python3-pip
      - python-pip

install_pipenv:
  pip.installed:
    - name: pipenv
    - upgrade: True
    - pip_bin: /usr/bin/pip3