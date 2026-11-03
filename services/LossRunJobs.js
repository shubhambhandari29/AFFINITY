import { Box, Button, Grid, Stack, useTheme } from "@mui/material";
import Loader from "../../../ui/Loader";
import { DataGrid, GridFooter } from "@mui/x-data-grid";
import { useState } from "react";
import api from "../../../../api/api";
import Swal from "sweetalert2";
import JobDetails from "./JobDetails";
import { MdSync } from "react-icons/md";
import useIntervalFetch from "../../../../hooks/useIntervalFetch";
import { GridFooterContainer } from "@mui/x-data-grid";
import { useNavigate } from "react-router-dom";

function CustomFooter({ manualRefresh, loading }) {
  const navigate = useNavigate();
  return (
    <GridFooterContainer sx={{ flexWrap: "wrap", gap: 1, px: 1, py: 1 }}>
      {/* Custom buttons on the left */}
      <Stack
        direction="row"
        spacing={2}
        useFlexGap
        sx={{ flexWrap: "wrap" }}
      >
        <Button
          variant="contained"
          size="small"
          onClick={manualRefresh}
          disabled={loading}
        >
          Refresh
        </Button>
        <Button variant="contained" size="small" onClick={() => navigate(-1)}>
          Back
        </Button>
      </Stack>

      {/* Standard MUI Pagination on the right */}
      <GridFooter sx={{
        border: 0,
        minWidth: 0,
        maxWidth: "100%",
        flexWrap: "wrap",
        "& .MuiTablePagination-root": { maxWidth: "100%", overflowX: "auto" },
      }} />
    </GridFooterContainer>
  );
}

export default function LossRunJobs() {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(false);
  const theme = useTheme();
  const [openJob, setOpenJob] = useState("");

  // --- Columns Definition ---
  const columns = [
    {
      field: "jobId",
      headerName: "Job ID",
      flex: 1.8,
      minWidth: 220,
    },
    {
      field: "jobType",
      headerName: "Job Type",
      width: 100,
    },
    {
      field: "reportType",
      headerName: "Report Type",
      width: 170,
      renderCell: (params) =>
        params.value === "claim_review" ? "Claim Review" : "Standard Loss Run",
    },
    {
      field: "lossDateFrom",
      headerName: "Loss Date From",
      width: 130,
      renderCell: (params) => params.value || (params.row.policyEffectiveDateFrom ? `Legacy policy: ${params.row.policyEffectiveDateFrom}` : "Default"),
    },
    {
      field: "requestedBy",
      headerName: "Requested By",
      flex: 1,
      minWidth: 180,
    },
    {
      field: "status",
      headerName: "Status",
      flex: 1,
      minWidth: 190,
      renderCell: (params) => {
        if (params.value === "processing") {
          return (
            <Box
              sx={{
                display: "inline-flex",
                alignItems: "center",
                height: "100%",
                minWidth: 0,
                "& svg": {
                  animation: "spin 2s linear infinite",
                },
                "@keyframes spin": {
                  "0%": { transform: "rotate(0deg)" },
                  "100%": { transform: "rotate(360deg)" },
                },
                color: theme.palette.background.green,
              }}
            >
              <span style={{ color: theme.palette.text.primary, marginRight: "10px" }}>
                {params.row.cancelRequested ? "Cancelling" : params.value}
              </span>
              <MdSync size={20} style={{ flexShrink: 0 }} />
            </Box>
          );
        }
        return params.value;
      },
    },
    {
      field: "phase",
      headerName: "Phase",
      width: 300,
      renderCell: (params) => (
        <Box component="span" title={params.row.progressMessage || params.value}
          sx={{ display: "block", width: "100%", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {params.row.progressMessage || params.value}
        </Box>
      ),
    },
    {
      field: "createdAt",
      headerName: "Created At",
      flex: 1,
      minWidth: 210,
    },
    {
      field: "completedAt",
      headerName: "Completed At",
      flex: 1,
      minWidth: 210,
    },
  ];

  const fetchData = async () => {
    setLoading(true);
    try {
      const res = await api.get("/loss_run/jobs");
      setJobs(res.data);
    } catch (err) {
      console.error(err);
      Swal.fire({
        title: "Error",
        text: "Some error occoured, unable to load jobs",
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
      setLoading(false);
    }
  };

  const hasActiveJob = jobs.some((job) => ["queued", "processing"].includes(job.status));
  const manualRefresh = useIntervalFetch(fetchData, hasActiveJob ? 10000 : 300000);

  return (
    <>
      {/*Modal for view policies  */}
      <JobDetails
        open={!!openJob}
        onClose={() => setOpenJob("")}
        jobData={jobs.find((job) => job.jobId === openJob)}
        onRefresh={manualRefresh}
      />

      <Grid sx={{ flexGrow: 1, minWidth: 0, width: "100%" }}>
        <div style={{ height: 600, minWidth: 0, width: "100%" }}>
          {loading && jobs.length === 0 ? (
            <Loader size={40} height={400} />
          ) : (
            <DataGrid
              rows={jobs}
              columns={columns}
              loading={loading}
              initialState={{
                pagination: { paginationModel: { page: 0, pageSize: 25 } },
              }}
              pageSizeOptions={[25, 50, 100]}
              getRowId={(row) => row.jobId}
              onRowClick={(row) => setOpenJob(row.id)}
              sx={{
                "& .MuiDataGrid-columnHeaders": {
                  backgroundColor: "#e9ecef",
                  fontSize: "12px",
                  textAlign: "center",
                },
                "& .MuiDataGrid-columnHeaderTitle": {
                  whiteSpace: "normal",
                  wordBreak: "break-word",
                },
              }}
              slots={{
                footer: CustomFooter,
              }}
              slotProps={{
                footer: {
                  manualRefresh: manualRefresh,
                  loading: loading,
                },
              }}
            />
          )}
        </div>
      </Grid>
    </>
  );
}
