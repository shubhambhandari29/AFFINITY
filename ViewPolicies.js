import { DataGrid } from "@mui/x-data-grid";
import {
  Box,
  Button,
  TextField,
  Typography,
  Stack,
  Grid,
  Tooltip,
  Zoom,
  useTheme,
  Autocomplete,
} from "@mui/material";
import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import api from "../../../../../api/api";
import Loader from "../../../../ui/Loader";
import dayjs from "dayjs";
import Swal from "sweetalert2";
import useDropdownData from "../../../../../hooks/useDropdownData";

const columns = [
  { field: "AccountName", headerName: "Name On Policy", flex: 2 },
  { field: "PolPref", headerName: "Sym", flex: 1 },
  { field: "PolicyNum", headerName: "PolNum", flex: 1 },
  { field: "PolMod", headerName: "Mod", flex: 1 },
  {
    field: "InceptDate",
    headerName: "InceptDate",
    flex: 1,
    type: "date",
    valueGetter: (value) => {
      if (!value) return null;
      const dateObj = dayjs(value, "DD-MM-YYYY");
      return dateObj.isValid() ? dateObj.toDate() : null;
    },
    valueFormatter: (value) => {
      if (!value) return "";
      return dayjs(value).format("DD-MM-YYYY");
    },
  },
  {
    field: "ExpDate",
    headerName: "Expires",
    flex: 1,
    type: "date",
    valueGetter: (value) => {
      if (!value) return null;
      const dateObj = dayjs(value, "DD-MM-YYYY");
      return dateObj.isValid() ? dateObj.toDate() : null;
    },
    valueFormatter: (value) => {
      if (!value) return "";
      return dayjs(value).format("DD-MM-YYYY");
    },
  },
  { field: "PolicyType", headerName: "PolicyType", flex: 2 },
  { field: "PolicyStatus", headerName: "PolicyStatus", flex: 1.5 },
];

export default function ViewPolicies({
  CustomerNum,
  CustomerName,
  isStepper,
  onCreatePolicy,
  onViewPolicy,
  onDataFetch,
  disableforDirector,
}) {
  const [policyList, setPolicyList] = useState([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [policyFilter, setPolicyFilter] = useState("");
  const [policyTypeFilter, setPolicyTypeFilter] = useState("");
  const [rowSelectionModel, setRowSelectionModel] = useState({});
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const [loading, setLoading] = useState("");
  const theme = useTheme();
  const { data: ddData, loading: ddLoading } =
    useDropdownData("/dropdowns/all");

  const boxStyles = {
    px: 4,
    py: 2,
    border: "1px solid lightgrey",
    borderRadius: "15px",
    boxShadow: "0 1px 8px rgba(0, 0, 0, 0.25)",
  };

  const filteredRows = policyList.filter((row) => {
    return (
      (statusFilter ? row.PolicyStatus === statusFilter : true) &&
      (policyFilter ? row.PolicyNum.includes(policyFilter) : true) &&
      (policyTypeFilter ? row.PolicyType.includes(policyTypeFilter) : true)
    );
  });

  //To always show latest mod first
  // filteredRows.sort((a, b) => b.PolMod - a.PolMod);

  // first sort based on PolicyNum & then PolMod
  filteredRows.sort((a, b) => {
    const numComparison = a.PolicyNum.localeCompare(b.PolicyNum, undefined, {
      numeric: true,
    });
    if (numComparison !== 0) return numComparison;
    return b.PolMod.localeCompare(a.PolMod, undefined, { numeric: true });
  });

  const handleGoToPolicy = () => {
    const row = policyList.filter(
      (row) => row.PK_Number === [...rowSelectionModel.ids][0],
    )[0];

    if (isStepper) onViewPolicy(row.PK_Number);
    else {
      const url = pathname.includes("cct")
        ? `/cct-view-policy/PK_Number=${row.PK_Number}?CustomerNum=${CustomerNum}&CustomerName=${CustomerName}`
        : `/view-policy/PK_Number=${row.PK_Number}`;

      navigate(url, { state: { from: pathname }, replace: true });
    }
  };

  useEffect(() => {
    const getPolicies = async () => {
      setLoading("fetching");
      try {
        const res = await api.get("/sac_policies/", {
          params: { CustomerNum },
        });
        setPolicyList(res.data);

        if (isStepper && onDataFetch) onDataFetch(res.data.length);
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

    getPolicies();
  }, [CustomerNum, isStepper, onDataFetch, theme.palette.error.main]);

  return (
    <Box sx={isStepper && boxStyles}>
      <Grid
        container
        justifyContent="space-between"
        sx={{
          mb: "20px",
          p: "0px 20px 10px",
          borderBottom: "1px solid lightgrey",
        }}
      >
        <Grid container direction="column">
          <Typography variant="h6" gutterBottom>
            Account Policies
          </Typography>
          <Typography variant="body2" color="text.secondary">
            All documented policies on file
          </Typography>
        </Grid>

        <Grid container direction="column" alignItems="center">
          <Typography variant="h6" display="inline">
            Account Name:
          </Typography>
          <Typography variant="h6" color="primary" display="inline">
            {CustomerName}
          </Typography>
        </Grid>

        <Grid container direction="column" alignItems="center" pr="30px">
          <Typography variant="h6" display="inline">
            Customer #:
          </Typography>
          <Typography variant="h6" color="primary" display="inline">
            {CustomerNum}
          </Typography>
        </Grid>
      </Grid>

      <Stack direction="row" spacing={2} sx={{ mb: 2, justifyContent: "end" }}>
        <Autocomplete
          options={
            ddData
              ?.filter((i) => i.DD_Type === "PolicyType")
              .sort((a, b) => a.DD_SortOrder - b.DD_SortOrder) || []
          }
          loading={ddLoading}
          getOptionLabel={(option) => option.DD_Value || ""}
          isOptionEqualToValue={(option, val) =>
            option.DD_Value === val?.DD_Value
          }
          onChange={(_, newValue) => {
            setPolicyTypeFilter(newValue ? newValue.DD_Value : "");
          }}
          renderInput={(params) => (
            <TextField {...params} label="Filter by Policy Type" />
          )}
          sx={{ minWidth: 200 }}
        />

        <Autocomplete
          options={
            ddData
              ?.filter((i) => i.DD_Type === "PolicyStatus")
              .sort((a, b) => a.DD_SortOrder - b.DD_SortOrder) || []
          }
          loading={ddLoading}
          getOptionLabel={(option) => option.DD_Value || ""}
          isOptionEqualToValue={(option, val) =>
            option.DD_Value === val?.DD_Value
          }
          onChange={(_, newValue) => {
            setStatusFilter(newValue ? newValue.DD_Value : "");
          }}
          renderInput={(params) => (
            <TextField {...params} label="Filter by Policy Status" />
          )}
          sx={{ minWidth: 250 }}
        />

        <TextField
          label="Filter by Policy Number"
          value={policyFilter}
          onChange={(e) => setPolicyFilter(e.target.value)}
          sx={{ minWidth: 200 }}
        />

        <Button
          variant="outlined"
          onClick={() => {
            setPolicyTypeFilter("");
            setStatusFilter("");
            setPolicyFilter("");
          }}
        >
          Reset Filters
        </Button>
      </Stack>

      <div
        style={{
          height: isStepper ? 350 : 400,
          width: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        {loading ? (
          <Loader size={40} height={isStepper ? 300 : 350} />
        ) : (
          <DataGrid
            rows={filteredRows}
            columns={columns}
            pageSize={5}
            rowsPerPageOptions={[5, 10]}
            checkboxSelection
            disableMultipleRowSelection
            getRowId={(row) => row.PK_Number}
            selectionModel={rowSelectionModel}
            onRowSelectionModelChange={(newSelection) =>
              setRowSelectionModel(newSelection)
            }
            localeText={{
              noRowsLabel: "No Policies Associated with this Account ",
            }}
            initialState={{
              sorting: {
                sortModel: [
                  {
                    field: "ExpDate",
                    sort: "desc",
                  },
                ],
              },
            }}
          />
        )}
      </div>

      <Stack
        direction="row"
        spacing={2}
        justifyContent="flex-end"
        sx={{ mt: 2 }}
      >
        {!pathname.includes("cct") && (
          <Button
            variant="contained"
            color="primary"
            onClick={() => {
              if (isStepper) onCreatePolicy();
              else {
                navigate(
                  `/create-new-policy?CustomerNum=${CustomerNum}&CustomerName=${CustomerName}`,
                  { state: { from: pathname }, replace: true },
                );
              }
            }}
            disabled={disableforDirector}
          >
            Create a New Policy
          </Button>
        )}
        <Tooltip
          title={!rowSelectionModel.ids?.size && "Please select a policy first"}
          placement="top"
          TransitionComponent={Zoom}
        >
          <span>
            <Button
              variant="contained"
              color="primary"
              disabled={!rowSelectionModel.ids?.size}
              onClick={handleGoToPolicy}
            >
              Go To Selected Policy
            </Button>
          </span>
        </Tooltip>
      </Stack>
    </Box>
  );
}
