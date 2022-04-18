
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ices_vms_data_call

Directory structure

    ├── data        Temporary R-binary files
    ├── delivery    The annexes to upload to ICES
    ├── logs        The log outputs
    ├── R           The scrips

Run the following in terminal:

    nohup R < R/logbooks.R --vanilla > logs/logbooks_YYYY-MM-DD.log &
    nohup R < R/stk.R --vanilla > logs/stk_YYYY-MM-DD.log &
    nohup R < R/annexes.R --vanilla > logs/anexes_YYYY-MM-DD.log &
