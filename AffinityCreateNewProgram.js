import {
  TextField,
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  Grid,
  Button,
  Tab,
  Tabs,
  useTheme,
  CircularProgress,
  Paper,
  Autocomplete,
} from "@mui/material";
import { forwardRef, useEffect, useImperativeHandle, useState } from "react";
import { useForm, Controller, FormProvider } from "react-hook-form";
import TabPanel from "../../../../ui/TabPanel";
import ProgramGeneral from "./ProgramGeneral";
import Notes from "../Notes";
import Shi from "../Shi";
import LossRunScheduling from "../LossRunScheduling";
import ClaimReviewScheduling from "../ClaimReviewScheduling";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import api from "../../../../../api/api";
import Loader from "../../../../ui/Loader";
import Swal from "sweetalert2";
import { useSelector } from "react-redux";
import Modal from "../../../../ui/Modal";
import ViewPolicyTypes from "../view-policy-types/ViewPolicyTypes";
import { affinityProgramFieldPermissions } from "../../../../../field-permissions";
import { formatDate } from "../../../../../util";
import useDropdownData from "../../../../../hooks/useDropdownData";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import dayjs from "dayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import CurrencyField from "../../../../ui/CurrencyField";

const defaultValues = {
  ProgramName: "",
  AcctStatus: "Active",
  SpecAcct1: "",
  LossCtl1: "",
  SpecAcct2: "Jenna Houle",
  LossCtl2: "Jenna Houle",
  DtCreated: new Date().toISOString().split("T")[0],
  AcctOwner: "",
  RiskSolMgr: "Jenna Houle",
  OnBoardDt: "",
  BusType: "Affinity",
  OBMethod: "",
  TotalPrem: "",
  AccomType: "",
  TermCause: "",
  TermDate: "",
  BranchVal: "",
  DateNotif: "",
  //programGeneral
  ServReq: "",
  ExceptYN: "",
  ExceptType: "",
  AcctNotes: "",
  //lossRunScheduling
  LossRunDistFreq: "",
  LossRunCheckboxes: [
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
    { checked: false, lastSendDate: "", NoClaims: false, AdHocReport: false },
  ],
  //lossRunNotes
  LossRunNotes: "",
  //claimReviewScheduling
  ClaimRevDistFreq: "",
  CRThresh: "50000",
  ClaimRevCheckboxes: [
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
    {
      checked: false,
      lastSendDate: "",
      reportType: "",
      deliveryMethod: "",
      narrativesProcessed: "",
    },
  ],
  //claimReviewNotes
  ClaimRevNotes: "",
  //shi
  SHIComplete: "Yes",
  SHINotes: "",
  //changeNotes
  ChangeNotes: "",
};

const AffinityCreateNewProgram = forwardRef((props, ref) => {
  const methods = useForm({ defaultValues });
  const { handleSubmit, control, reset, getValues, setValue } = methods;
  const [tabIndex, setTabIndex] = useState(0);
  const [viewPolicyTypes, setViewPolicyTypes] = useState(false);
  const { column_name } = useParams();
  const [loading, setLoading] = useState(false);
  const user = useSelector((state) => state.auth.user);
  const allowedFields = affinityProgramFieldPermissions[user.role];
  const { pathname, state } = useLocation();
  const from = state?.from || "/pending-items";
  const [submitOrSave, setSubmitOrSave] = useState("");
  const navigate = useNavigate();
  const theme = useTheme();
  const { isStepper, formData } = props;
  const [accountStatus, setAccountStatus] = useState("");
  const { data: SpecAcctData, loading: SpecAcctLoading } = useDropdownData(
    "/dropdowns/SAC_Contact1",
  );
  const { data: AcctOwnerData, loading: AcctOwnerLoading } = useDropdownData(
    "/dropdowns/AcctOwner",
  );
  const { data: LossCtlData, loading: LossCtlLoading } =
    useDropdownData("/dropdowns/LossCtl");
  const { data: BranchNameData, loading: BranchNameLoading } = useDropdownData(
    "/dropdowns/BranchName",
  );
  const { data: ddData, loading: ddLoading } =
    useDropdownData("/dropdowns/all");
  const disableforDirector =
    user.role === "Director" &&
    getValues("Stage") === "Admin" &&
    getValues("IsSubmitted") === 1;

  const isEnabled = (fieldName) => {
    if (allowedFields === "ALL") return true;
    return allowedFields?.includes(fieldName);
  };

  const tabList = [
    {
      label: "Program General",
      component: <ProgramGeneral isEnabled={isEnabled} formData={formData} />,
    },
    {
      label: "Loss Run Scheduling",
      component: <LossRunScheduling isEnabled={isEnabled} />,
    },
    {
      label: "Loss Run Notes",
      component: (
        <Notes
          name="LossRunNotes"
          label="Loss Run Notes"
          isEnabled={isEnabled}
        />
      ),
    },
    {
      label: "Claim Review Scheduling",
      component: <ClaimReviewScheduling isEnabled={isEnabled} />,
    },
    {
      label: "Claim Review Notes",
      component: (
        <Notes
          name="ClaimRevNotes"
          label="Claim Review Notes"
          isEnabled={isEnabled}
        />
      ),
    },
    {
      label: "SHI",
      component: <Shi isEnabled={isEnabled} />,
    },
    {
      label: "Change Notes",
      component: (
        <Notes name="ChangeNotes" label="Change Notes" isEnabled={isEnabled} />
      ),
    },
  ];

  useImperativeHandle(ref, () => ({
    submit: async (actionType) => {
      let resData = false;
      await handleSubmit(async (data) => {
        const mockEvent = { nativeEvent: { submitter: { name: actionType } } };
        const result = await onSubmit(data, mockEvent);
        resData = result;
      }, onError)();
      return resData;
    },
  }));

  const onSubmit = async (data, e) => {
    const operation = e.nativeEvent.submitter.name;

    //confirmation alert on submission
    if (operation === "submit") {
      const res = await Swal.fire({
        title: "Confirm Submit",
        text:
          user.role === "Underwriter"
            ? "This will save all the changes to the account, is that what you want to do?"
            : "This will save all the changes & close the account. Is that what you want to do?",
        icon: "warning",
        showCancelButton: true,
        confirmButtonText: "Yes",
        cancelButtonText: "No",
        iconColor: theme.palette.warning.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
        cancelButtonColor: theme.palette.error.main,
      });

      if (res.dismiss === Swal.DismissReason.cancel) return;
    }

    let updatedData =
      operation === "save"
        ? {
            ...data,
            Stage: user.role,
            IsSubmitted:
              pathname !== "/affinity-create-new-program" &&
              state?.from !== "/pending-items"
                ? 1
                : 0,
          }
        : { ...data, Stage: user.role, IsSubmitted: 1 };

    const LossRunCheckboxes =
      updatedData.LossRunCheckboxes &&
      updatedData.LossRunCheckboxes.map((item, index) => ({
        ProgramName: updatedData.ProgramName,
        MthNum: index + 1,
        RptMth: item.checked,
        CompDate: item.lastSendDate,
        NoClaims: item.NoClaims,
        AdHocReport: item.AdHocReport,
      }));

    const ClaimRevCheckboxes =
      updatedData.ClaimRevCheckboxes &&
      updatedData.ClaimRevCheckboxes.map((item, index) => ({
        ProgramName: updatedData.ProgramName,
        MthNum: index + 1,
        RptMth: item.checked,
        CompDate: item.lastSendDate,
        RptType: item.reportType,
        DelivMeth: item.deliveryMethod,
        CRNumNarr: +item.narrativesProcessed,
      }));

    delete updatedData.LossRunCheckboxes;
    delete updatedData.ClaimRevCheckboxes;

    updatedData = {
      ...updatedData,
      TotalPrem: String(updatedData.TotalPrem) && "0.00",
    };

    setSubmitOrSave(operation);

    try {
      //Check if same customer already exists or not, in case of inserts
      if (
        user.role === "Underwriter" &&
        pathname.includes("affinity-create-new") &&
        !formData
      ) {
        const resSac = await api.get("/affinity_program/", {
          params: { ProgramName: updatedData.ProgramName },
        });

        if (resSac.status === 200 && resSac.data?.length > 0) {
          Swal.fire({
            title: "Data Validation Error",
            text: "This Program Name already exists, duplicate records are not permitted",
            icon: "error",
            confirmButtonText: "OK",
            iconColor: theme.palette.error.main,
            customClass: {
              confirmButton: "swal-confirm-button",
              cancelButton: "swal-cancel-button",
            },
            buttonsStyling: false,
          });
          return;
        }
      }

      //Continue saving if customer number does not exist
      const apiCalls = [api.post("/affinity_program/upsert", updatedData)];

      if (LossRunCheckboxes) {
        apiCalls.push(
          api.post("/loss_run_frequency_affinity/upsert", LossRunCheckboxes),
        );
      }
      if (ClaimRevCheckboxes) {
        apiCalls.push(
          api.post(
            "/claim_review_frequency_affinity/upsert",
            ClaimRevCheckboxes,
          ),
        );
      }

      const responses = await Promise.all(apiCalls);
      const allSuccessful = responses.every((res) => res.status === 200);

      if (!allSuccessful)
        throw new Error("Some error occoured, unable to save the data");

      setSubmitOrSave("");

      const res = await Swal.fire({
        title: "Changes Saved",
        text: "Your changes have been saved",
        icon: "success",
        confirmButtonText: "OK",
        iconColor: theme.palette.success.main,
        customClass: {
          confirmButton: "swal-confirm-button",
          cancelButton: "swal-cancel-button",
        },
        buttonsStyling: false,
      });

      if (res.isConfirmed || res.dismiss) {
        if (isStepper) return updatedData;
        else if (operation === "submit") {
          reset(defaultValues);
          navigate("/pending-items", { replace: true });
        }
        return true;
      }
    } catch (err) {
      console.error(err);
      Swal.fire({
        title: "Error",
        text: "Some error occoured, unable to save the data",
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
      setSubmitOrSave("");
    }
  };

  const onError = (errors) => {
    const errVals = Object.values(errors);

    if (errVals.length > 0) {
      Swal.fire({
        title: "Data Validation Error",
        text: errVals[0].message,
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

  const handleTabChange = (_, newValue) => setTabIndex(newValue);

  useEffect(() => {
    const fetchData = async (searchByColumn, value) => {
      try {
        setLoading(true);
        const [main, lossRun, claimReview] = await Promise.all([
          api.get("/affinity_program/", {
            params: { [searchByColumn]: value },
          }),
          api.get("/loss_run_frequency_affinity/", {
            params: { ProgramName: value },
          }),
          api.get("/claim_review_frequency_affinity/", {
            params: { ProgramName: value },
          }),
        ]);

        //replacing all null values with empty string
        const data = Object.fromEntries(
          Object.entries(main.data[0]).map(([k, v]) => [
            k,
            v === null ? "" : v,
          ]),
        );

        let formattedData = {
          ...data,
          DtCreated: formatDate(data.DtCreated),
          DateNotif: formatDate(data.DateNotif),
          OnBoardDt: formatDate(data.OnBoardDt),
          TermDate: formatDate(data.TermDate),
        };

        let LossRunCheckboxes = [];
        let ClaimRevCheckboxes = [];

        for (let i = 1; i <= 12; i++) {
          LossRunCheckboxes.push({
            checked: false,
            lastSendDate: "",
            NoClaims: false,
            AdHocReport: false,
          });
          ClaimRevCheckboxes.push({
            checked: false,
            lastSendDate: "",
            reportType: "",
            deliveryMethod: "",
            narrativesProcessed: "",
          });
        }

        if (lossRun.data.length > 0) {
          lossRun.data.forEach((element) => {
            LossRunCheckboxes[element.MthNum - 1].checked =
              element.RptMth === 1;
            LossRunCheckboxes[element.MthNum - 1].lastSendDate = formatDate(
              element.CompDate,
            );
            LossRunCheckboxes[element.MthNum - 1].NoClaims = element.NoClaims;
            LossRunCheckboxes[element.MthNum - 1].AdHocReport =
              element.AdHocReport;
          });
          formattedData.LossRunCheckboxes = LossRunCheckboxes;
        }

        if (claimReview.data.length > 0) {
          claimReview.data.forEach((element) => {
            ClaimRevCheckboxes[element.MthNum - 1].checked =
              element.RptMth === 1;
            ClaimRevCheckboxes[element.MthNum - 1].lastSendDate = formatDate(
              element.CompDate,
            );
            ClaimRevCheckboxes[element.MthNum - 1].reportType = element.RptType;
            ClaimRevCheckboxes[element.MthNum - 1].deliveryMethod =
              element.DelivMeth;
            ClaimRevCheckboxes[element.MthNum - 1].narrativesProcessed =
              element.CRNumNarr;
          });
          formattedData.ClaimRevCheckboxes = ClaimRevCheckboxes;
        }

        setAccountStatus(formattedData.AcctStatus);
        reset(formattedData);
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
        setLoading(false);
      }
    };

    if (column_name) {
      const [key, value] = column_name.split("=");
      fetchData(key, value);
    }

    if (isStepper && formData) fetchData("ProgramName", formData.ProgramName);

    reset(defaultValues);
  }, [column_name, reset, formData, isStepper, theme.palette.error.main]);

  useEffect(() => {
    if (pathname === "/affinity-create-new-program") setAccountStatus("Active");
    if (from.includes("policy") && !isStepper) setViewPolicyTypes(true);
  }, [pathname, from, isStepper]);

  const handleBack = async () => {
    const res = await Swal.fire({
      title: "Confirm Request to Cancel",
      text: "Any unsaved changes will be lost, are you sure you want to do this?",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Yes",
      cancelButtonText: "No",
      iconColor: theme.palette.warning.main,
      customClass: {
        confirmButton: "swal-confirm-button",
        cancelButton: "swal-cancel-button",
      },
      buttonsStyling: false,
      cancelButtonColor: theme.palette.error.main,
    });

    if (res.isConfirmed) navigate("/pending-items", { replace: true });
  };

  const handleDistributionUpdate = async (oldVal, val, field) => {
    const newVal = (field === "AcctOwner" ? AcctOwnerData : LossCtlData).find(
      (i) => i[field === "AcctOwner" ? "SACName" : "RepName"] === val,
    );
    if (!newVal) return;

    const data = {
      ProgramName: getValues("ProgramName"),
      RecipCat:
        field === "AcctOwner" ? "Account Owner" : "Risk Solutions Consultant",
      EMailAddress: newVal[field === "AcctOwner" ? "EMailID" : "LCEmail"],
      DistVia: "Email",
      AttnTo: newVal[field === "AcctOwner" ? "SACName" : "RepName"],
    };

    try {
      await Promise.allSettled([
        api.post("/claim_review_distribution_affinity/delete", [
          { ProgramName: getValues("ProgramName"), AttnTo: oldVal },
        ]),
        api.post("/loss_run_distribution_affinity/delete", [
          { ProgramName: getValues("ProgramName"), AttnTo: oldVal },
        ]),
        api.post("/claim_review_distribution_affinity/upsert", [data]),
        api.post("/loss_run_distribution_affinity/upsert", [data]),
      ]);
    } catch (error) {
      console.error(error);
      Swal.fire({
        title: "Error",
        text: "Some error occoured, unable to add the data to Recipient Lists",
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

  const handleAccountStatus = (e) => {
    const selectedVal = e.target.value;
    setAccountStatus(selectedVal);
    setValue("AcctStatus", selectedVal);
    if (selectedVal !== "Inactive" && selectedVal !== "In Runoff") {
      setValue("DateNotif", null);
      setValue("TermDate", null);
      setValue("TermCause", "");
    }
  };

  if (loading) return <Loader size={40} height="600px" />;

  return (
    <>
      {pathname !== "/affinity-create-new-program" && (
        //Modal for view policy types
        <Modal
          open={viewPolicyTypes}
          onClose={() => setViewPolicyTypes(false)}
          maxWidth="lg"
        >
          <ViewPolicyTypes
            ProgramName={getValues("ProgramName")}
            disableforDirector={disableforDirector}
          />
        </Modal>
      )}

      <FormProvider {...methods}>
        <form
          onSubmit={handleSubmit(onSubmit, onError)}
          noValidate
          style={{ height: "100%" }}
        >
          <Grid
            container
            spacing={1.5}
            sx={{ height: "100%", alignContent: "flex-start" }}
          >
            {/* Main Form Container */}
            <Grid size={{ xs: 12 }}>
              <Paper
                sx={{
                  p: 2,
                  backgroundColor: "#EBF0F8",
                  border: "1px solid #e0e0e0",
                }}
              >
                <Grid container spacing={1}>
                  <Grid size={{ xs: 12, md: 9 }}>
                    <Controller
                      name="ProgramName"
                      control={control}
                      rules={{
                        required:
                          "Program Name is mandatory and cannot be empty",
                      }}
                      render={({ field, fieldState: { error } }) => (
                        <TextField
                          {...field}
                          fullWidth
                          label="Affinity Program Name"
                          disabled={
                            !(
                              user.role === "Underwriter" &&
                              pathname.includes("affinity-create-new") &&
                              !formData
                            )
                          }
                          required
                          error={!!error}
                        />
                      )}
                    />
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <InputLabel>Account Status</InputLabel>
                      <Controller
                        name="AcctStatus"
                        control={control}
                        render={({ field }) => (
                          <Select
                            {...field}
                            label="Account Status"
                            disabled={!isEnabled("AcctStatus")}
                            onChange={(e) => handleAccountStatus(e)}
                            renderValue={(selected) => {
                              const existingOption = [
                                "Active",
                                "Inactive",
                                "in Runoff",
                                "Unique",
                              ].find((opt) => opt === selected);
                              return existingOption ? existingOption : selected;
                            }}
                          >
                            <MenuItem value="Active">Active</MenuItem>
                            <MenuItem value="Inactive">Inactive</MenuItem>
                            <MenuItem value="In Runoff">In Runoff</MenuItem>
                            <MenuItem value="Unique">Unique</MenuItem>
                          </Select>
                        )}
                      />
                    </FormControl>
                  </Grid>

                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="SpecAcct1"
                        control={control}
                        disabled={!isEnabled("SpecAcct1")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={SpecAcctData || []}
                            loading={SpecAcctLoading}
                            getOptionLabel={(option) => option.SACName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.SACName === val?.SACName
                            }
                            value={
                              value
                                ? SpecAcctData.find(
                                    (opt) => opt.SACName === value,
                                  ) || { SACName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.SACName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Spec Accounts 1"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="LossCtl1"
                        control={control}
                        disabled={!isEnabled("LossCtl1")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={LossCtlData || []}
                            loading={LossCtlLoading}
                            getOptionLabel={(option) => option.RepName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.RepName === val?.RepName
                            }
                            value={
                              value
                                ? LossCtlData.find(
                                    (opt) => opt.RepName === value,
                                  ) || { RepName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              const selectedvalue = newValue
                                ? newValue.RepName
                                : "";
                              onChange(selectedvalue);
                              handleDistributionUpdate(
                                value,
                                selectedvalue,
                                "LossCtl1",
                              );
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Risk Solutions 1"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <InputLabel>Business Type</InputLabel>
                      <Controller
                        name="BusType"
                        control={control}
                        render={({ field }) => (
                          <Select
                            {...field}
                            label="Business Type"
                            disabled={!isEnabled("BusType")}
                            MenuProps={{
                              PaperProps: { sx: { maxHeight: 300 } },
                            }}
                            renderValue={(selected) => {
                              const existingOption = ddData
                                .filter((i) => i.DD_Type === "BusinessType")
                                .find((opt) => opt.value === selected);
                              return existingOption
                                ? existingOption.label
                                : selected;
                            }}
                          >
                            {ddLoading ? (
                              <MenuItem disabled>
                                <Loader size={15} height="20px" />
                              </MenuItem>
                            ) : (
                              ddData
                                .filter((i) => i.DD_Type === "BusinessType")
                                .sort((a, b) => a - b)
                                .map((i) => (
                                  <MenuItem key={i.DD_Value} value={i.DD_Value}>
                                    {i.DD_Value}
                                  </MenuItem>
                                ))
                            )}
                          </Select>
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="DtCreated"
                        control={control}
                        render={({
                          field: { onChange, value, ...fieldProps },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="Created Date"
                            value={value ? dayjs(value) : null}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                            disabled={!isEnabled("DtCreated")}
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>

                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="SpecAcct2"
                        control={control}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={SpecAcctData || []}
                            loading={SpecAcctLoading}
                            disabled={!isEnabled("SpecAcct2")}
                            getOptionLabel={(option) => option.SACName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.SACName === val?.SACName
                            }
                            value={
                              value
                                ? SpecAcctData.find(
                                    (opt) => opt.SACName === value,
                                  ) || { SACName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.SACName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Spec Accounts 2"
                                inputRef={ref}
                                disabled={!isEnabled("SpecAcct2")}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="LossCtl2"
                        control={control}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={LossCtlData || []}
                            loading={LossCtlLoading}
                            disabled={!isEnabled("LossCtl2")}
                            getOptionLabel={(option) => option.RepName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.RepName === val?.RepName
                            }
                            value={
                              value
                                ? LossCtlData.find(
                                    (opt) => opt.RepName === value,
                                  ) || { RepName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.RepName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Risk Solutions 2"
                                inputRef={ref}
                                disabled={!isEnabled("LossCtl2")}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <InputLabel>Onboarding Method</InputLabel>
                      <Controller
                        name="OBMethod"
                        control={control}
                        disabled={!isEnabled("OBMethod")}
                        render={({ field }) => (
                          <Select {...field} label="Onboarding Method">
                            <MenuItem value="In Person">In Person</MenuItem>
                            <MenuItem value="Remote">Remote</MenuItem>
                            <MenuItem value="BOR Change">BOR Change</MenuItem>
                          </Select>
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="TermCause"
                        control={control}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            disabled={
                              !isEnabled("TermCause") ||
                              (isEnabled("TermCause") &&
                                accountStatus !== "Inactive" &&
                                accountStatus !== "In Runoff")
                            }
                            options={
                              ddData
                                .filter((i) => i.DD_Type === "DNRStatus")
                                .sort((a, b) => a - b) || []
                            }
                            loading={ddLoading}
                            getOptionLabel={(option) => option.DD_Value || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.DD_Value === val?.DD_Value
                            }
                            value={
                              value
                                ? ddData.find(
                                    (opt) => opt.DD_Value === value,
                                  ) || { DD_Value: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.DD_Value : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Term Cause"
                                inputRef={ref}
                                disabled={
                                  !isEnabled("TermCause") ||
                                  (isEnabled("TermCause") &&
                                    accountStatus !== "Inactive" &&
                                    accountStatus !== "In Runoff")
                                }
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>

                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="AcctOwner"
                        control={control}
                        disabled={!isEnabled("AcctOwner")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={AcctOwnerData || []}
                            loading={AcctOwnerLoading}
                            getOptionLabel={(option) => option.SACName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.SACName === val?.SACName
                            }
                            value={
                              value
                                ? AcctOwnerData.find(
                                    (opt) => opt.SACName === value,
                                  ) || { SACName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              const selectedvalue = newValue
                                ? newValue.SACName
                                : "";
                              onChange(selectedvalue);
                              handleDistributionUpdate(
                                value,
                                selectedvalue,
                                "AcctOwner",
                              );
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Account Owner"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="RiskSolMgr"
                        control={control}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={LossCtlData || []}
                            loading={LossCtlLoading}
                            disabled={!isEnabled("RiskSolMgr")}
                            getOptionLabel={(option) => option.RepName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.RepName === val?.RepName
                            }
                            value={
                              value
                                ? LossCtlData.find(
                                    (opt) => opt.RepName === value,
                                  ) || { RepName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.RepName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Risk Solutions Mgr"
                                inputRef={ref}
                                disabled={!isEnabled("RiskSolMgr")}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="OnBoardDt"
                        control={control}
                        rules={{
                          required:
                            "On Board Date is mandatory and cannot be empty",
                        }}
                        render={({
                          field: { onChange, value, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="On Board Date"
                            value={value ? dayjs(value) : null}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                            disabled={!isEnabled("OnBoardDt")}
                            slotProps={{
                              textField: { required: true, error: !!error },
                            }}
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="TermDate"
                        control={control}
                        render={({
                          field: { onChange, value, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="Termination Date"
                            value={value ? dayjs(value) : null}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                            slotProps={{
                              textField: { error: !!error },
                            }}
                            disabled={
                              !isEnabled("TermDate") ||
                              (isEnabled("TermDate") &&
                                accountStatus !== "Inactive" &&
                                accountStatus !== "In Runoff")
                            }
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>

                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="BranchVal"
                        control={control}
                        disabled={!isEnabled("BranchVal")}
                        rules={{
                          required:
                            "Branch Name is mandatory and cannot be empty",
                        }}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={BranchNameData || []}
                            loading={BranchNameLoading}
                            getOptionLabel={(option) => option.BranchName || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.BranchName === val?.BranchName
                            }
                            value={
                              value
                                ? BranchNameData.find(
                                    (opt) => opt.BranchName === value,
                                  ) || { BranchName: value }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.BranchName : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Branch Name"
                                inputRef={ref}
                                required
                                error={!!error}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <FormControl fullWidth>
                      <Controller
                        name="AccomType"
                        control={control}
                        disabled={!isEnabled("AccomType")}
                        render={({
                          field: { onChange, value, ref, ...fieldProps },
                        }) => (
                          <Autocomplete
                            {...fieldProps}
                            options={
                              ddData
                                .filter((i) => i.DD_Type === "AccomType")
                                .sort((a, b) => a - b) || []
                            }
                            loading={ddLoading}
                            getOptionLabel={(option) => option.DD_Value || ""}
                            isOptionEqualToValue={(option, val) =>
                              option.DD_Value === val?.DD_Value
                            }
                            value={
                              value
                                ? ddData.find(
                                    (opt) => opt.DD_Value === value,
                                  ) || {
                                    DD_Value: value,
                                  }
                                : null
                            }
                            onChange={(_, newValue) => {
                              onChange(newValue ? newValue.DD_Value : "");
                            }}
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                label="Accommodation Type"
                                inputRef={ref}
                              />
                            )}
                          />
                        )}
                      />
                    </FormControl>
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <CurrencyField
                      name="TotalPrem"
                      control={control}
                      disabled={!isEnabled("TotalPrem")}
                      label="Total $ Premium"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, md: 3 }}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                      <Controller
                        name="DateNotif"
                        control={control}
                        rules={{
                          required:
                            isEnabled("DateNotif") &&
                            getValues("AcctStatus") === "Inactive"
                              ? "Notification date is mandatory and cannot be empty"
                              : false,
                        }}
                        render={({
                          field: { onChange, value, ...fieldProps },
                          fieldState: { error },
                        }) => (
                          <DatePicker
                            {...fieldProps}
                            label="Notification Date"
                            value={value ? dayjs(value) : null}
                            onChange={(newValue) => {
                              onChange(
                                newValue ? newValue.format("YYYY-MM-DD") : null,
                              );
                            }}
                            slotProps={{
                              textField: { required: true, error: !!error },
                            }}
                            disabled={
                              !isEnabled("DateNotif") ||
                              (isEnabled("DateNotif") &&
                                accountStatus !== "Inactive" &&
                                accountStatus !== "In Runoff")
                            }
                          />
                        )}
                      />
                    </LocalizationProvider>
                  </Grid>
                </Grid>
              </Paper>
            </Grid>

            <Grid size={{ xs: 12 }} sx={{ width: "100%" }}>
              <Paper
                sx={{
                  pt: 2,
                  px: 2,
                  border: "1px solid #e0e0e0",
                }}
              >
                <Tabs
                  value={tabIndex}
                  onChange={handleTabChange}
                  variant="scrollable"
                  scrollButtons="auto"
                  aria-label="account tabs"
                >
                  {tabList.map((tab) => (
                    <Tab key={tab.label} label={tab.label} />
                  ))}
                </Tabs>

                {tabList.map((tab, i) => (
                  <TabPanel key={i} value={tabIndex} index={i}>
                    {tab.component}
                  </TabPanel>
                ))}
              </Paper>
            </Grid>

            {!isStepper && (
              <Grid
                sx={{
                  width: "100%",
                  display: "flex",
                  justifyContent: "space-between",
                  marginTop: "auto",
                }}
              >
                <Grid
                  sx={{
                    display: "flex",
                    gap: 2,
                  }}
                >
                  {pathname !== "/affinity-create-new-program" && (
                    <Button
                      variant="outlined"
                      color="primary"
                      onClick={() => setViewPolicyTypes(true)}
                    >
                      View Policy Types
                    </Button>
                  )}
                </Grid>
                <Grid
                  sx={{
                    display: "flex",
                    gap: 2,
                  }}
                >
                  <Button
                    type="button"
                    variant="outlined"
                    disabled={submitOrSave !== ""}
                    onClick={handleBack}
                  >
                    Back
                  </Button>
                  <Button
                    type="submit"
                    variant="outlined"
                    color="primary"
                    name="save"
                    disabled={submitOrSave !== "" || disableforDirector}
                  >
                    {submitOrSave === "save" ? (
                      <CircularProgress size={20} color="inherit" />
                    ) : (
                      "Save"
                    )}
                  </Button>
                  <Button
                    type="submit"
                    variant="contained"
                    color="primary"
                    name="submit"
                    disabled={submitOrSave !== "" || disableforDirector}
                  >
                    {submitOrSave === "submit" ? (
                      <CircularProgress size={20} color="inherit" />
                    ) : (
                      "Submit"
                    )}
                  </Button>
                </Grid>
              </Grid>
            )}
          </Grid>
        </form>
      </FormProvider>
    </>
  );
});

export default AffinityCreateNewProgram;
