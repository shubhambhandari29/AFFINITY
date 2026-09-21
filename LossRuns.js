import {
  Alert,
  AlertTitle,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  FormControlLabel,
  RadioGroup,
  Radio,
  FormLabel,
  TextField,
  useTheme,
  Button,
  Grid,
  Tooltip,
} from "@mui/material";
import { DataGrid } from "@mui/x-data-grid";
import { useEffect, useState } from "react";
import api from "../../../../api/api";
import Loader from "../../../ui/Loader";
import Swal from "sweetalert2";
import { useNavigate } from "react-router-dom";

export default function LossRuns() {
  const [accounts, setAccounts] = useState([]);
  const [loading, setLoading] = useState("");
  const [searchBy, setSearchBy] = useState("AccountName");
  const [searchValue, setSearchValue] = useState("");
  const [rowSelectionModel, setRowSelectionModel] = useState([]);
  const [lossRunAll, setLossRunAll] = useState(true);
  const [reportType, setReportType] = useState("standard");
  const [lossDateFrom, setLossDateFrom] = useState("");
  const theme = useTheme();
  const [...selectedIds] = rowSelectionModel?.ids || "";
  const navigate = useNavigate();

  // Dynamic mapping exactly as it was in the original LossRuns
  const dynamicSearchBy =
    searchBy === "AccountName"
      ? "Customer Name"
      : searchBy === "PolicyNameInsured"
        ? "Name Insured on Policy"
        : searchBy.replace(/([a-z])([A-Z])/g, "$1 $2").replace("Num", "Number");

  useEffect(() => {
    const loadOptions = async () => {
      setLoading("fetching");
      try {
        const res = await api.get(`/loss_run/accounts`, {
          params: { search_by: searchBy },
        });

        if (res.status === 200) {
          const formattedData = res.data.map((item) => {
            const formattedItem = {
              ...item,
              id:
                item["Customer Number"] +
                "-" +
                Math.floor(100000000 + Math.random() * 900000000),
            };

            // Replicate the Service Level slicing from your original component
            if (formattedItem["Service Level"]) {
              formattedItem["Service Level"] =
                formattedItem["Service Level"].slice(2);
            }
            return formattedItem;
          });

          setAccounts(formattedData);
        }
      } catch (err) {
        console.error(err);
        Swal.fire({
          title: "Error",
          text: "Some error occoured, unable to load data",
          icon: "error",
          confirmButtonText: "OK",
          iconColor: theme.palette.error.main,
          customClass: {
            confirmButton: "swal-confirm-button",
            cancelButton: "swal-cancel-button",
          },
          buttonsStyling: false,
        });
      } finally {
        setLoading("");
      }
    };

    if (searchBy) {
      loadOptions();
      setSearchValue("");
      setRowSelectionModel([]);
    }
  }, [searchBy, theme.palette.error.main]);

  // Client-side filtering logic based on ViewPolicies
  const filteredRows = accounts.filter((row) => {
    if (!searchValue) return true;

    // Safely grab the dynamically selected column value and compare it
    const cellValue = row[dynamicSearchBy]
      ? String(row[dynamicSearchBy]).toLowerCase()
      : "";
    return cellValue.includes(searchValue.toLowerCase());
  });

  // Dynamically generate DataGrid columns based on the keys of the first returned object
  const columns =
    accounts.length > 0
      ? Object.keys(accounts[0])
          .filter((key) => key !== "id" && key !== "header")
          .map((key) => ({
            field: key,
            headerName: key,
            flex: 1,
            minWidth: 150,
          }))
      : [];

  const handleLossRunTrigger = async () => {
    const url = lossRunAll ? "loss_run/generate-all" : "loss_run/generate";
    const payload = {
      reportType,
      ...(lossDateFrom && { lossDateFrom }),
      ...(!lossRunAll && {
        customerNumbers: selectedIds.map((i) => i.split("-")[0]),
      }),
    };
    try {
      const res = await api.post(url, payload);
      Swal.fire({
        title: !res.data.message.includes("running") ? "Success" : "Error",
        text: res.data.message,
        icon: !res.data.message.includes("running") ? "success" : "error",
        confirmButtonText: "OK",
        iconColor: !res.data.message.includes("running")
          ? theme.palette.success.main
          : theme.palette.error.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });
    } catch (error) {
      console.error(error);
      Swal.fire({
        title: "Error",
        text: "Some error occoured, unable to trigger loss run",
        icon: "error",
        confirmButtonText: "OK",
        iconColor: theme.palette.error.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });
    }
  };

  return (
    <Grid
      container
      spacing={2}
      sx={{ display: "grid", placeItems: "center", mt: 2 }}
    >
      <Alert
        severity="info"
        variant="outlined"
        role="note"
        sx={{
          width: "100%",
          boxSizing: "border-box",
          borderColor: "primary.main",
          "& .MuiAlert-icon": { color: "primary.main" },
        }}
      >
        <AlertTitle>Automatic monthly reports</AlertTitle>
        When enabled, Standard Loss Runs are scheduled for the <strong>1st</strong>
        {" "}and Claim Review reports for the <strong>20th</strong> of each month,
        at <strong>12:00 AM Eastern Time</strong> (adjusted for daylight saving).
        {" "}Use <strong>View Existing Jobs</strong> below to check progress and
        download completed reports. If another report is running, the scheduled
        run will wait.
      </Alert>
      <Grid container spacing={1}>
        <FormControl sx={{ minWidth: 220 }}>
          <InputLabel id="report-type-label">Report Type</InputLabel>
          <Select
            labelId="report-type-label"
            label="Report Type"
            value={reportType}
            onChange={(e) => setReportType(e.target.value)}
          >
            <MenuItem value="standard">Standard Loss Run</MenuItem>
            <MenuItem value="claim_review">Claim Review</MenuItem>
          </Select>
        </FormControl>
        <TextField
          id="loss-date-from"
          label="Loss date from (optional)"
          type="date"
          value={lossDateFrom}
          onChange={(e) => setLossDateFrom(e.target.value)}
          InputLabelProps={{ shrink: true }}
          helperText="Includes losses from this date through the report date, plus older open claims and claims with outstanding loss reserves. Applies to all accounts in this request. Leave blank for default history."
          sx={{ width: 300, maxWidth: "100%" }}
        />
        {lossDateFrom && (
          <Button
            type="button"
            size="small"
            onClick={() => setLossDateFrom("")}
          >
            Clear date
          </Button>
        )}
        <FormControl
          component="fieldset"
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            gap: 2,
          }}
        >
          <FormLabel id="LossRunAll">
            Do you want to run for all the accounts?
          </FormLabel>
          <RadioGroup
            row
            defaultValue="Yes"
            value={lossRunAll ? "Yes" : "No"}
            onChange={(e) => {
              setLossRunAll(e.target.value === "Yes");
              setRowSelectionModel([]);
            }}
          >
            <FormControlLabel value="Yes" control={<Radio />} label="Yes" />
            <FormControlLabel value="No" control={<Radio />} label="No" />
          </RadioGroup>
        </FormControl>
        {!(lossRunAll || (!lossRunAll && selectedIds.length > 0)) ? (
          <Tooltip title="Please select an account first" arrow>
            <span>
              <Button
                variant="contained"
                color="primary"
                disabled={
                  !(lossRunAll || (!lossRunAll && selectedIds.length > 0))
                }
                onClick={handleLossRunTrigger}
              >
                Trigger Loss Run
              </Button>
            </span>
          </Tooltip>
        ) : (
          <Button
            variant="contained"
            color="primary"
            disabled={!(lossRunAll || (!lossRunAll && selectedIds.length > 0))}
            onClick={handleLossRunTrigger}
          >
            Trigger Loss Run
          </Button>
        )}
        <Button
          variant="contained"
          color="primary"
          onClick={() => navigate("/loss-run-jobs")}
        >
          View Existing Jobs
        </Button>
      </Grid>

      {!lossRunAll && (
        <Grid
          container
          spacing={2}
          size={12}
          sx={{ display: "flex", direction: "row", justifyContent: "center" }}
        >
          <Grid size={3}>
            <FormControl fullWidth>
              <InputLabel>Search By</InputLabel>
              <Select
                label="Search By"
                value={searchBy}
                onChange={(e) => setSearchBy(e.target.value)}
              >
                <MenuItem value="AccountName">Account Name</MenuItem>
                <MenuItem value="CustomerNum">Customer Number</MenuItem>
                <MenuItem value="PolicyNum">Policy Number</MenuItem>
                <MenuItem value="ProducerCode">Producer Code</MenuItem>
                <MenuItem value="AffiliateName">Affiliate Name</MenuItem>
                <MenuItem value="PolicyNameInsured">
                  Policy Name Insured
                </MenuItem>
              </Select>
            </FormControl>
          </Grid>

          <Grid size={3}>
            <TextField
              fullWidth
              label={`Filter by ${dynamicSearchBy}`}
              value={searchValue}
              onChange={(e) => setSearchValue(e.target.value)}
              placeholder="Type here to filter..."
            />
          </Grid>
        </Grid>
      )}

      {!lossRunAll && (
        <Grid
          sx={{
            height: 500,
            width: "100%",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid lightgrey",
            borderRadius: "10px",
            padding: "5px",
          }}
        >
          {loading === "fetching" ? (
            <Loader size={40} height={500} />
          ) : (
            <DataGrid
              getRowId={(row) => row.id}
              rows={filteredRows}
              columns={columns}
              pageSize={10}
              rowsPerPageOptions={[25, 50, 100]}
              initialState={{
                pagination: { paginationModel: { pageSize: 25 } },
              }}
              checkboxSelection
              keepNonExistentRowsSelected
              selectionModel={rowSelectionModel}
              onRowSelectionModelChange={(newSelection) => {
                setRowSelectionModel(newSelection);
              }}
              localeText={{
                noRowsLabel: "No Accounts Found",
              }}
            />
          )}
        </Grid>
      )}
    </Grid>
  );
}
