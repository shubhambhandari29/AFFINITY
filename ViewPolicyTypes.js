import { DataGrid } from "@mui/x-data-grid";
import {
  Box,
  Button,
  Typography,
  Stack,
  Grid,
  Tooltip,
  Zoom,
  useTheme,
} from "@mui/material";
import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import api from "../../../../../api/api";
import Loader from "../../../../ui/Loader";
import Swal from "sweetalert2";

const columns = [
  { field: "ProgramName", headerName: "Program Name", flex: 1 },
  { field: "PolicyType", headerName: "Policy Type", flex: 1 },
  { field: "AgentName", headerName: "Primary Agent", flex: 1 },
  { field: "AgentCode", headerName: "Agent Code", flex: 1 },
];

export default function ViewPolicyTypes({
  ProgramName,
  isStepper,
  onCreatePolicy,
  onViewPolicy,
  onDataFetch,
  disableforDirector,
}) {
  const [policyList, setPolicyList] = useState([]);
  const [rowSelectionModel, setRowSelectionModel] = useState({});
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const [loading, setLoading] = useState("");
  const theme = useTheme();

  const boxStyles = {
    px: 4,
    py: 2,
    border: "1px solid lightgrey",
    borderRadius: "15px",
    boxShadow: "0 1px 8px rgba(0, 0, 0, 0.25)",
  };

  const handleGoToPolicy = () => {
    const row = policyList.filter(
      (row) => row.PK_Number === [...rowSelectionModel.ids][0],
    )[0];

    if (isStepper) onViewPolicy(row.PK_Number);
    else {
      const url = pathname.includes("cct")
        ? `/cct-view-policy-types/PK_Number=${row.PK_Number}?ProgramName=${ProgramName}`
        : `/view-policy-types/PK_Number=${row.PK_Number}`;

      navigate(url, { state: { from: pathname }, replace: true });
    }
  };

  useEffect(() => {
    const getPolicies = async () => {
      setLoading("fetching");
      try {
        const res = await api.get("/affinity_policy_types/", {
          params: { ProgramName },
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
  }, [ProgramName, isStepper, onDataFetch, theme.palette.error.main]);

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
            Affinity Policy Types
          </Typography>
          <Typography variant="body2" color="text.secondary">
            A list of all documented policy types
          </Typography>
        </Grid>

        <Grid container direction="column" alignItems="center">
          <Typography variant="h6" display="inline">
            Account Name:
          </Typography>
          <Typography variant="h6" color="primary" display="inline">
            {ProgramName}
          </Typography>
        </Grid>
      </Grid>

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
            rows={policyList}
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
              noRowsLabel: "No Policy types Associated with this Account ",
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
                  `/create-new-policy-type/?ProgramName=${ProgramName}`,
                  { state: { from: pathname }, replace: true },
                );
              }
            }}
            disabled={disableforDirector}
          >
            Create a New Policy Type
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
              Go To Selected Policy Type
            </Button>
          </span>
        </Tooltip>
      </Stack>
    </Box>
  );
}
