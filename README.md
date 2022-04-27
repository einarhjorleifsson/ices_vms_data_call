
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ices_vms_data_call

Directory structure

    ├── data        Temporary R-binary files           - .gitignored
    ├── delivery    The annexes to upload to ICES      - .gitignored
    ├── logs        The log outputs
    ├── R           The scrips

Run the following sequentially in terminal (each process must be
completed prior the next step):

    nohup R < R/logbooks.R --vanilla > logs/logbooks_YYYY-MM-DD.log &
    nohup R < R/stk.R --vanilla > logs/stk_YYYY-MM-DD.log &
    nohup R < R/annexes.R --vanilla > logs/anexes_YYYY-MM-DD.log 

The files in `logs`-directory show the R-scripts used, messages and
package varsions used per date indicated in the file name.
