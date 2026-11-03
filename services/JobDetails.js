import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  Grid,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  useTheme,
  CircularProgress,
  Alert,
  GlobalStyles,
} from "@mui/material";
import { useState } from "react";
import { MdSync } from "react-icons/md";
import api from "../../../../api/api";
import Swal from "sweetalert2";

// Helper component for the metric boxes to keep the UI consistent with your toolbars
const MetricBox = ({ label, value, color = "#1a365d", bgColor = "white" }) => (
  <Box
    sx={{
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      justifyContent: "center",
      border: `1px solid ${color}`,
      borderRadius: "6px",
      p: 1.5,
      bgcolor: bgColor,
      minWidth: 0,
      boxSizing: "border-box",
      overflowWrap: "anywhere",
    }}
  >
    <Typography variant="caption" sx={{ fontWeight: "bold", color: "#666" }}>
      {label}
    </Typography>
    <Typography
      variant="h6"
      sx={{ fontWeight: "bold", color: color, lineHeight: 1.2 }}
    >
      {value}
    </Typography>
  </Box>
);

export default function JobDetails({ open, onClose, jobData, onRefresh }) {
  const theme = useTheme();
  const [isDownloading, setIsDownloading] = useState(false);
  const [isCancelling, setIsCancelling] = useState(false);
  const [cancelRequestedJobId, setCancelRequestedJobId] = useState(null);
  if (!jobData) return null;
  const isActive = ["queued", "processing"].includes(jobData.status);
  const cancellationPending = isActive && (
    jobData.cancelRequested || cancelRequestedJobId === jobData.jobId
  );

  const handleCancel = async () => {
    const jobId = jobData.jobId;
    const confirmation = await Swal.fire({
      title: "Cancel this loss run?",
      text: "Processing will stop when the current operation ends. Files already generated will remain available.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Cancel job",
      cancelButtonText: "Keep running",
      iconColor: theme.palette.warning.main,
      customClass: {
        confirmButton: "swal-confirm-button",
        cancelButton: "swal-cancel-button",
      },
      buttonsStyling: false,
    });
    if (!confirmation.isConfirmed) return;
    setIsCancelling(true);
    try {
      const response = await api.post(`/loss_run/jobs/${jobId}/cancel`);
      setCancelRequestedJobId(jobId);
      await Swal.fire({
        title: "Cancellation",
        text: response.data.message,
        icon: "info",
        confirmButtonText: "OK",
        iconColor: theme.palette.info.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });
    } catch (error) {
      await Swal.fire({
        title: "Unable to cancel",
        text: error.response?.data?.detail?.error || "Unable to request cancellation. Please refresh the job status.",
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
      setIsCancelling(false);
      if (onRefresh) await onRefresh();
    }
  };

  // Determine status color based on the job phase/status
  const getStatusColor = (status) => {
    switch (status?.toLowerCase()) {
      case "completed":
        return "success";
      case "partially_completed":
      case "cancelled":
        return "warning";
      case "failed":
        return "error";
      default:
        return "default";
    }
  };

  const handleDownload = async () => {
    setIsDownloading(true);

    try {
      const response = await api.get(
        `/loss_run/jobs/${jobData.jobId}/download`,
        {
          responseType: "blob",
        },
      );

      const contentDisposition = response.headers["content-disposition"];
      const filenameMatch = contentDisposition?.match(
        /filename\*?=(?:UTF-8''|")?([^";]+)/i,
      );

      let filename =
        jobData.generatedCount === 1
          ? `loss_run_${jobData.jobId}.xlsx`
          : `loss_run_${jobData.jobId}.zip`;

      if (filenameMatch?.[1]) {
        filename = decodeURIComponent(filenameMatch[1].replace(/"/g, ""));
      }

      // create blob link & trigger download natievly
      const downloadUrl = window.URL.createObjectURL(response.data);
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = filename;
      document.body.appendChild(link);

      // Cleanup DOM & memory
      link.click();
      link.remove();
      window.URL.revokeObjectURL(downloadUrl);
    } catch (error) {
      console.error(error);

      let message = "Unable to download the generated loss-run file.";
      if (error.response?.status === 409) {
        message = "This loss-run job is still being processed.";
      } else if (error.response?.status === 404) {
        message = "No generated files were found for this job.";
      }

      Swal.fire({
        title: "Download Failed",
        text: message,
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
      setIsDownloading(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      slotProps={{
        paper: {
          sx: {
            borderRadius: "8px",
            m: { xs: 1, sm: 4 },
            width: { xs: "calc(100% - 16px)", sm: "calc(100% - 64px)" },
            maxHeight: { xs: "calc(100dvh - 16px)", sm: "calc(100dvh - 64px)" },
          },
        },
      }}
    >
      <GlobalStyles
        styles={{ ".swal2-container": { zIndex: theme.zIndex.modal + 1 } }}
      />
      {/* Modal Header */}
      <DialogTitle
        sx={{
          bgcolor: theme.palette.background.offWhite,
          borderBottom: "1px solid #e0e0e0",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          flexWrap: "wrap",
          gap: 1,
          px: { xs: 2, sm: 3 },
        }}
      >
        <Box sx={{ minWidth: 0, flex: "1 1 220px" }}>
          <Typography
            variant="h6"
            component="div"
            sx={{ fontWeight: "bold", color: theme.palette.background.blue }}
          >
            Job Details
          </Typography>
          <Typography
            variant="caption"
            sx={{ color: theme.palette.text.secondary, wordBreak: "break-all" }}
          >
            ID: {jobData.jobId}
          </Typography>
        </Box>
        <Chip
          label={
            jobData.status === "processing" ? (
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
                <span style={{ color: theme.palette.text.primary, marginRight: "10px" }}>
                  {cancellationPending ? "CANCELLING" : jobData.status?.replace("_", " ").toUpperCase()}
                </span>
                <MdSync size={20} style={{ flexShrink: 0 }} />
              </Box>
            ) : (
              <Box>{jobData.status?.replace("_", " ").toUpperCase()}</Box>
            )
          }
          color={getStatusColor(jobData.status)}
          size="small"
          sx={{ fontWeight: "bold", borderRadius: "4px", maxWidth: "100%", flexShrink: 0 }}
        />
      </DialogTitle>

      <DialogContent sx={{ py: 3, px: { xs: 2, sm: 3 }, overflowWrap: "anywhere" }}>
        {(jobData.progressMessage || cancellationPending) && (
          <Alert severity={cancellationPending ? "warning" : "info"} sx={{ mt: 1, mb: 2 }}>
            {cancellationPending
              ? "Cancellation requested. Waiting for the current operation to stop."
              : jobData.progressMessage}
          </Alert>
        )}
        {/* Top Section: Core Metrics */}
        <Box sx={{ display: "grid", gridTemplateColumns: { xs: "repeat(2, minmax(0, 1fr))", sm: "repeat(4, minmax(0, 1fr))" }, gap: 2, mb: 4, mt: 1 }}>
          <MetricBox label="Requested" value={jobData.requestedCount} />
          <MetricBox label="Processed" value={jobData.processedCount} />
          <MetricBox
            label="Generated"
            value={jobData.generatedCount}
            color={theme.palette.background.green}
            bgColor="#edf7ed"
          />
          <MetricBox
            label="Failed"
            value={jobData.failedCount}
            color="#d32f2f"
            bgColor="#fdeded"
          />
        </Box>

        {/* Middle Section: Metadata Details */}
        <Box
          sx={{
            bgcolor: theme.palette.background.offWhite,
            p: 2,
            borderRadius: "8px",
            border: "1px solid #e0e0e0",
            mb: jobData.failedCount > 0 ? 4 : 0,
          }}
        >
          <Grid container spacing={2} sx={{ minWidth: 0 }}>
            <Grid size={{ xs: 12, sm: 6, md: 3 }} sx={{ minWidth: 0 }}>
              <Typography variant="caption" color="textSecondary" display="block">
                Report Type
              </Typography>
              <Typography variant="body2" fontWeight="bold">
                {jobData.reportType === "claim_review"
                  ? "Claim Review"
                  : "Standard Loss Run"}
              </Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }} sx={{ minWidth: 0 }}>
              <Typography variant="caption" color="textSecondary" display="block">
                Loss Date From
              </Typography>
              <Typography variant="body2" fontWeight="bold">
                {jobData.lossDateFrom || (jobData.policyEffectiveDateFrom ? `Legacy policy cutoff: ${jobData.policyEffectiveDateFrom}` : "Default history")}
              </Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }} sx={{ minWidth: 0 }}>
              <Typography
                variant="caption"
                color="textSecondary"
                display="block"
              >
                Requested By
              </Typography>
              <Typography variant="body2" fontWeight="bold">
                {jobData.requestedBy}
              </Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }} sx={{ minWidth: 0 }}>
              <Typography
                variant="caption"
                color="textSecondary"
                display="block"
              >
                Job Type
              </Typography>
              <Typography
                variant="body2"
                fontWeight="bold"
                textTransform="capitalize"
              >
                {jobData.jobType}
              </Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 6, md: 3 }} sx={{ minWidth: 0 }}>
              <Typography
                variant="caption"
                color="textSecondary"
                display="block"
              >
                Phase
              </Typography>
              <Typography
                variant="body2"
                fontWeight="bold"
                textTransform="capitalize"
              >
                {jobData.phase}
              </Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 6 }} sx={{ minWidth: 0 }}>
              <Typography
                variant="caption"
                color="textSecondary"
                display="block"
              >
                Error Message
              </Typography>
              <Typography
                variant="body2"
                fontWeight="bold"
                color={jobData.errorMessage ? "error" : "textPrimary"}
              >
                {jobData.errorMessage || "None"}
              </Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 4 }} sx={{ minWidth: 0 }}>
              <Typography
                variant="caption"
                color="textSecondary"
                display="block"
              >
                Created Date
              </Typography>
              <Typography variant="body2">{jobData.createdAt}</Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 4 }} sx={{ minWidth: 0 }}>
              <Typography
                variant="caption"
                color="textSecondary"
                display="block"
              >
                Started Date
              </Typography>
              <Typography variant="body2">{jobData.startedAt}</Typography>
            </Grid>
            <Grid size={{ xs: 12, sm: 4 }} sx={{ minWidth: 0 }}>
              <Typography
                variant="caption"
                color="textSecondary"
                display="block"
              >
                Completed Date
              </Typography>
              <Typography variant="body2">{jobData.completedAt}</Typography>
            </Grid>
          </Grid>
        </Box>

        {/* Bottom Section: Failures DataGrid (Conditional) */}
        {jobData.failedCount > 0 && (
          <Box>
            <Typography
              variant="subtitle2"
              sx={{
                fontWeight: "bold",
                mb: 1,
                color: theme.palette.background.red,
              }}
            >
              Failure Details ({jobData.failedCount})
            </Typography>

            <TableContainer
              component={Paper}
              variant="outlined"
              sx={{ maxHeight: 250, overflow: "auto", width: "100%" }}
            >
              <Table size="small" stickyHeader sx={{ minWidth: 380 }}>
                <TableHead>
                  <TableRow>
                    <TableCell
                      sx={{
                        fontWeight: "bold",
                        backgroundColor: theme.palette.background.grey2,
                        fontSize: "12px",
                      }}
                    >
                      ID
                    </TableCell>
                    <TableCell
                      sx={{
                        fontWeight: "bold",
                        backgroundColor: theme.palette.background.grey2,
                        fontSize: "12px",
                      }}
                    >
                      Customer Number
                    </TableCell>
                    <TableCell
                      sx={{
                        fontWeight: "bold",
                        backgroundColor: theme.palette.background.grey2,
                        fontSize: "12px",
                      }}
                    >
                      Reason
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {(jobData.failures || []).map((fail, index) => (
                    <TableRow key={index} hover>
                      <TableCell
                        sx={{ fontSize: "12px", verticalAlign: "top" }}
                      >
                        {index + 1}
                      </TableCell>
                      <TableCell
                        sx={{ fontSize: "12px", verticalAlign: "top" }}
                      >
                        {fail.customerNumber}
                      </TableCell>
                      <TableCell
                        sx={{
                          fontSize: "12px",
                          verticalAlign: "top",
                          wordBreak: "break-word",
                        }}
                      >
                        {fail.reason}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
        )}
      </DialogContent>

      <DialogActions
        sx={{
          p: 2,
          bgcolor: theme.palette.background.offWhite,
          borderTop: "1px solid #e0e0e0",
          flexWrap: "wrap",
          gap: 1,
          "& > :not(style) ~ :not(style)": { ml: 0 },
          "& .MuiButton-root": { minHeight: 36, whiteSpace: "nowrap" },
        }}
      >
        {isActive && (
          <Button
            variant="outlined"
            color="error"
            size="small"
            disabled={isCancelling || cancellationPending}
            onClick={handleCancel}
          >
            {isCancelling || cancellationPending ? "Cancelling…" : "Cancel Job"}
          </Button>
        )}
        <Button
          variant="contained"
          size="small"
          disabled={
            isDownloading ||
            jobData.generatedCount === 0 ||
            !["completed", "partially_completed", "cancelled"].includes(jobData.status)
          }
          onClick={handleDownload}
          sx={{ minWidth: 100 }}
        >
          {isDownloading ? <CircularProgress size={16} /> : "Download"}
        </Button>
        <Button onClick={onClose} variant="contained" size="small">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
}
