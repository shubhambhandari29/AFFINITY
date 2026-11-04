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
import { useNavigate, useSearchParams } from "react-router-dom";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import dayjs from "dayjs";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";

export default function LossRuns() {
  const [searchParams, setSearchParams] = useSearchParams();
  const businessType = searchParams.get("businessType") === "affinity" ? "affinity" : "sac";
  const isAffinity = businessType === "affinity";
  const apiBase = isAffinity ? "affinity_loss_run" : "loss_run";
  const [accounts, setAccounts] = useState([]);
  const [loading, setLoading] = useState("");
  const [searchBy, setSearchBy] = useState("AccountName");
  const [searchValue, setSearchValue] = useState("");
  const [rowSelectionModel, setRowSelectionModel] = useState([]);
  const [lossRunAll, setLossRunAll] = useState(true);
  const [reportType, setReportType] = useState("standard");
  const [lossDateFrom, setLossDateFrom] = useState("");
  const theme = useTheme();
  const selectedIds = Array.isArray(rowSelectionModel)
    ? rowSelectionModel
    : rowSelectionModel?.type === "exclude"
      ? accounts.filter((row) => !rowSelectionModel.ids.has(row.id)).map((row) => row.id)
      : Array.from(rowSelectionModel?.ids || []);
  const navigate = useNavigate();

  // Dynamic mapping exactly as it was in the original LossRuns
  const dynamicSearchBy =
    isAffinity ? (searchBy === "CustomerNum" ? "Program ID" : searchBy === "ProducerCode" ? "Agent Codes" : "Program Name") : searchBy === "AccountName"
      ? "Customer Name"
      : searchBy === "PolicyNameInsured"
        ? "Name Insured on Policy"
        : searchBy.replace(/([a-z])([A-Z])/g, "$1 $2").replace("Num", "Number");

  useEffect(() => {
    let active = true;
    const loadOptions = async () => {
      setLoading("fetching");
      try {
        const res = await api.get(isAffinity ? "/affinity_loss_run/programs" : `/loss_run/accounts`, {
          params: { search_by: searchBy },
        });

        if (active && res.status === 200) {
          const formattedData = res.data.map((item) => {
            if (isAffinity) {
              return {
                id: item.programId,
                "Program ID": item.programId,
                "Program Name": item.programName,
                "Account Status": item.accountStatus,
                "Loss Run Frequency": item.lossRunFrequency,
                "Agent Codes": item.agentCodes.join(", "),
              };
            }
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
        if (!active) return;
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
        if (active) setLoading("");
      }
    };

    if (searchBy) {
      loadOptions();
      setSearchValue("");
      setRowSelectionModel([]);
      setAccounts([]);
    }
    return () => { active = false; };
  }, [searchBy, isAffinity, theme.palette.error.main]);

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
    if (!isAffinity && lossDateFrom && !dayjs(lossDateFrom).isValid()) {
      await Swal.fire({
        title: "Invalid date",
        text: "Please select a valid loss date or clear it for default history.",
        icon: "error",
      });
      return;
    }
    const url = `${apiBase}/${lossRunAll ? "generate-all" : "generate"}`;
    const payload = isAffinity ? (lossRunAll ? undefined : { programIds: selectedIds.map(Number) }) : {
      reportType,
      // Send the selected calendar day, not a UTC timestamp from Day.js JSON.
      ...(lossDateFrom && {
        lossDateFrom: dayjs(lossDateFrom).format("YYYY-MM-DD"),
      }),
      ...(!lossRunAll && {
        customerNumbers: selectedIds.map((i) => String(i).split("-")[0]),
      }),
    };
    try {
      const res = await api.post(url, payload);
      const result = await Swal.fire({
        title: !res.data.message.includes("running") ? "Success" : "Error",
        text: res.data.message,
        icon: !res.data.message.includes("running") ? "success" : "error",
        confirmButtonText: "OK",
        showDenyButton: true,
        denyButtonText: "View Jobs",
        iconColor: !res.data.message.includes("running")
          ? theme.palette.success.main
          : theme.palette.error.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });
      if (result.isDenied) {
        navigate(`/loss-run-jobs?businessType=${res.data.businessType || businessType}`);
      }
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
    <Grid container spacing={2} sx={{ width: "100%", minWidth: 0 }}>
      <FormControl sx={{ minWidth: 220, alignSelf: "start" }}>
        <InputLabel id="loss-run-business-label">Loss Run For</InputLabel>
        <Select labelId="loss-run-business-label" label="Loss Run For" value={businessType}
          onChange={(e) => {
            setAccounts([]);
            setRowSelectionModel([]);
            setSearchBy("AccountName");
            setSearchValue("");
            setReportType("standard");
            setLossDateFrom("");
            setSearchParams({ businessType: e.target.value });
          }}>
          <MenuItem value="sac">SAC</MenuItem>
          <MenuItem value="affinity">Affinity</MenuItem>
        </Select>
      </FormControl>
      {isAffinity ? (
        <Alert severity="info" variant="outlined" sx={{ width: "100%", boxSizing: "border-box" }}>
          Affinity generates one workbook per program using its agents marked LossRunReq = Yes.
          Selected and all-program runs are available. Scheduling, claim review and extended dates
          are not enabled for Affinity. Use View Existing Jobs to check progress and download reports.
        </Alert>
      ) : <Alert
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
        When enabled, Standard Loss Runs are scheduled for the{" "}
        <strong>1st</strong> and Claim Review reports for the{" "}
        <strong>20th</strong> of each month, at{" "}
        <strong>12:00 AM Eastern Time</strong> (adjusted for daylight saving).{" "}
        Use <strong>View Existing Jobs</strong> below to check progress and
        download completed reports. If another report is running, the scheduled
        run will wait.
      </Alert>}
      <Grid container spacing={2} size={12} sx={{ alignItems: "flex-start", minWidth: 0 }}>
        <Grid size={{ xs: 12, sm: 6, lg: 2 }} sx={{ minWidth: 0 }}>
          <FormControl fullWidth>
            <InputLabel id="report-type-label">Report Type</InputLabel>
            <Select
              labelId="report-type-label"
              label="Report Type"
              value={isAffinity ? "standard" : reportType}
              disabled={isAffinity}
              onChange={(e) => setReportType(e.target.value)}
            >
              <MenuItem value="standard">Standard Loss Run</MenuItem>
              <MenuItem value="claim_review">Claim Review</MenuItem>
            </Select>
          </FormControl>
        </Grid>
        {!isAffinity && <Grid size={{ xs: 12, sm: 6, lg: 4 }} sx={{ minWidth: 0 }}>
          <LocalizationProvider dateAdapter={AdapterDayjs}>
            <DatePicker
              label="Loss date from (optional)"
              value={lossDateFrom ? dayjs(lossDateFrom) : null}
              onChange={(newValue) => setLossDateFrom(newValue)}
              slotProps={{
                textField: {
                  fullWidth: true,
                  helperText:
                    "Includes losses from this date through the report date, plus older open claims and claims with outstanding loss reserves. Applies to all accounts in this request. Leave blank for default history.",
                },
                field: { clearable: true },
              }}
            />
          </LocalizationProvider>
        </Grid>}
        <Grid size={{ xs: 12, sm: 6, lg: 3 }} sx={{ minWidth: 0 }}>
          <FormControl
            component="fieldset"
            sx={{
              display: "flex",
              flexDirection: "column",
              alignItems: { xs: "flex-start", lg: "center" },
              gap: 1,
            }}
          >
            <FormLabel id="LossRunAll">
              Do you want to run for all the {isAffinity ? "programs" : "accounts"}?
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
        </Grid>
        <Grid size={{ xs: 12, sm: 6, lg: 3 }} sx={{ display: "flex", gap: 1, flexWrap: "wrap", alignItems: "flex-start", "& .MuiButton-root": { minHeight: 40 } }}>
          {!(lossRunAll || (!lossRunAll && selectedIds.length > 0)) ? (
            <Tooltip
              title={isAffinity ? "Please select a program first" : "Please select an account first"}
              arrow
              placement="top"
            >
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
              disabled={
                !(lossRunAll || (!lossRunAll && selectedIds.length > 0))
              }
              onClick={handleLossRunTrigger}
            >
              Trigger Loss Run
            </Button>
          )}
          <Button
            variant="contained"
            color="primary"
            onClick={() => navigate(`/loss-run-jobs?businessType=${businessType}`)}
          >
            View Existing Jobs
          </Button>
        </Grid>
      </Grid>

      {!lossRunAll && (
        <Grid
          container
          spacing={2}
          size={12}
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "center",
            mt: 1,
          }}
        >
          <Grid size={{ xs: 12, sm: 6, md: 3 }}>
            <FormControl fullWidth>
              <InputLabel>Search By</InputLabel>
              <Select
                label="Search By"
                value={searchBy}
                onChange={(e) => setSearchBy(e.target.value)}
              >
                <MenuItem value="AccountName">{isAffinity ? "Program Name" : "Account Name"}</MenuItem>
                <MenuItem value="CustomerNum">{isAffinity ? "Program ID" : "Customer Number"}</MenuItem>
                {!isAffinity && <MenuItem value="PolicyNum">Policy Number</MenuItem>}
                <MenuItem value="ProducerCode">Producer Code</MenuItem>
                {!isAffinity && <MenuItem value="AffiliateName">Affiliate Name</MenuItem>}
                {!isAffinity && <MenuItem value="PolicyNameInsured">
                  Policy Name Insured
                </MenuItem>}
              </Select>
            </FormControl>
          </Grid>

          <Grid size={{ xs: 12, sm: 6, md: 3 }}>
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
          size={12}
          sx={{
            height: 380,
            width: "100%",
            minWidth: 0,
            boxSizing: "border-box",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid lightgrey",
            borderRadius: "10px",
            padding: "5px",
          }}
        >
          {loading === "fetching" ? (
            <Loader size={40} height={360} />
          ) : (
            <DataGrid
              key={`${businessType}-${searchBy}-${lossRunAll}`}
              getRowId={(row) => row.id}
              rows={filteredRows}
              columns={columns}
              pageSize={10}
              rowsPerPageOptions={[25, 50, 100]}
              initialState={{
                pagination: { paginationModel: { pageSize: 25 } },
              }}
              checkboxSelection
              disableRowSelectionExcludeModel
              keepNonExistentRowsSelected
              selectionModel={rowSelectionModel}
              onRowSelectionModelChange={(newSelection) => {
                setRowSelectionModel(newSelection);
              }}
              localeText={{
                noRowsLabel: isAffinity ? "No Programs Found" : "No Accounts Found",
              }}
            />
          )}
        </Grid>
      )}
    </Grid>
  );
}
