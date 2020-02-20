# California Renewables Curtailment Study

## Quickstart

1.  [Install DVC](https://dvc.org/doc/install)
2.  Configure dvc gdrive remote (please take note of the `--local` flag!): `dvc remote modify data gdrive_client_secret <secret_key> --local`.  The associated data will not be public during the study itself.
3.  Pull data: `dvc pull`
4.  Run the analysis (pending): `dvc repro`

## System Dependencies

Debian/Linux

```
sudo apt install libeccodes0
```

MacOS

```
brew install eccodes
```