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
    <GridFooterContainer>
      {/* Custom buttons on the left */}
      <Stack
        direction="row"
        spacing={2}
        sx={{ pl: 2, pt: 1, flexWrap: "wrap" }}
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
      <GridFooter />
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
      field: "policyEffectiveDateFrom",
      headerName: "History From",
      width: 130,
      renderCell: (params) => params.value || "Default",
    },
    {
      field: "requestedBy",
      headerName: "Requested By",
      flex: 1,
    },
    {
      field: "status",
      headerName: "Status",
      flex: 1,
      renderCell: (params) => {
        if (params.value === "processing") {
          return (
            <Box
              sx={{
                display: "inline-flex",
                alignItems: "center",
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
              <span style={{ color: "black", marginRight: "10px" }}>
                {params.value}
              </span>
              <MdSync size={20} />
            </Box>
          );
        }
        return params.value;
      },
    },
    {
      field: "phase",
      headerName: "Phase",
      width: 100,
    },
    {
      field: "createdAt",
      headerName: "Created At",
      flex: 1,
    },
    {
      field: "completedAt",
      headerName: "Completed At",
      flex: 1,
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

  const manualRefresh = useIntervalFetch(fetchData, 300000); // 300,000ms = 5 mins

  return (
    <>
      {/*Modal for view policies  */}
      <JobDetails
        open={!!openJob}
        onClose={() => setOpenJob("")}
        jobData={jobs.find((job) => job.jobId === openJob)}
      />

      <Grid sx={{ flexGrow: 1, minWidth: 0 }}>
        <div style={{ height: 600, minWidth: "950px" }}>
          {loading ? (
            <Loader size={40} height={400} />
          ) : (
            <DataGrid
              rows={jobs}
              columns={columns}
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
