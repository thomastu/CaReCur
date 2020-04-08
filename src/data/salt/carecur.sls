create_venv:
  cmd.run:
    - unless: pipenv --venv
    - cwd: ~/CaReCur
    - name: pipenv install
