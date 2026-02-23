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
  Box,
  CircularProgress,
  useTheme,
  Autocomplete,
} from "@mui/material";
import { forwardRef, useEffect, useImperativeHandle, useState } from "react";
import { useForm, Controller, FormProvider } from "react-hook-form";
import TabPanel from "../../../../ui/TabPanel";
import GeneralProgram from "./GeneralProgram";
import InsuredContacts from "./InsuredContacts";
import ClaimHandling1 from "./ClaimHandling1";
import ClaimHandling3 from "./ClaimHandling3";
import ClaimHandling2 from "./ClaimHandling2";
import CctInstructionsOther from "./CctInstructionsOther";
import CctAssignment from "./CctAssignment";
import ShiPrint from "../../../ShiPrint";
import {
  useLocation,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import Swal from "sweetalert2";
import Loader from "../../../../ui/Loader";
import api from "../../../../../api/api";
import { policyTypeFieldPermissions } from "../../../../../field-permissions";
import { useSelector } from "react-redux";
import useDropdownData from "../../../../../hooks/useDropdownData";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import dayjs from "dayjs";
import { formatDate } from "../../../../../util";

const defaultValues = {
  ProgramName: "",
  PolicyType: "",
  PolicyStatus: "Active",
  DateCreated: new Date().toISOString().split("T")[0],
  PolicyBusinessType: "Affinity",
  UnderwriterName: "",
  UWMgr: "",
  LOCCoded: "",
  TermReason: "",
  TermDate: "",
  //generalProgram
  PolicyNotes: "",
  //insuredContacts
  InsuredContact1: "",
  InsuredPhone1: "",
  InsuredCell1: "",
  InsuredEMail1: "",
  InsuredContact2: "",
  InsuredPhone2: "",
  InsuredCell2: "",
  InsuredEMail2: "",
  InsuredNotes: "",
  //claimHandling
  ContactInstruct: "",
  CoverageInstruct: "",
  // claimHandling2
  PrefCounselYN: "",
  LitigationInstruct: "",
  // claimHandling3
  RecoveryInstruct: "",
  MiscCovInstruct: "",
  // cctInstructionsOther
  AcctLocID: "",
  AcctLocNotes: "",
  AcctProdClaims: "",
  AcctValetCov: "",
  CCTOtherNotes: "",
  AddLDocs: "No",
  // cctAssignment
  SpecHand: "Auto Assign",
  CCTAssgInstruct: "",
};

const CreateNewPolicyTypes = forwardRef((props, ref) => {
  const methods = useForm({ defaultValues });
  const { handleSubmit, control, getValues, setValue, reset } = methods;
  const [tabIndex, setTabIndex] = useState(0);
  const { column_name } = useParams();
  const navigate = useNavigate();
  const [submitOrSave, setSubmitOrSave] = useState("");
  const { pathname, state } = useLocation();
  const from = state?.from || "/pending-items";
  const theme = useTheme();
  const [searchParams] = useSearchParams();
  const [loading, setLoading] = useState(false);
  const user = useSelector((state) => state.auth.user);
  const allowedFields = policyTypeFieldPermissions[user.role];
  const {
    isStepper,
    accountData,
    pkProp,
    onReturn,
    onCreatePolicy,
    onModStart,
  } = props;
  const [isNextMod, setIsNextMod] = useState(false);
  const { data: ddData, loading: ddLoading } =
    useDropdownData("/dropdowns/all");
  const { data: UnderwritersData, loading: UnderwritersLoading } =
    useDropdownData("/dropdowns/Underwriters");
  const { data: TermReasonData, loading: TermReasonLoading } = useDropdownData(
    "/dropdowns/DNRStatus",
  );
  const [affinityData, setAffinityData] = useState({});
  const disableForDirector =
    user.role === "Director" &&
    affinityData?.Stage === "Admin" &&
    affinityData?.IsSubmitted === 1;

  const isEnabled = (fieldName) => {
    if (allowedFields === "ALL") return true;
    return allowedFields?.includes(fieldName);
  };

  const tabList = [
    {
      label: "General Information",
      component: <GeneralProgram isEnabled={isEnabled} />,
    },
    {
      label: "Insured Contacts",
      component: (
        <InsuredContacts
          isEnabled={isEnabled}
          disableForDirector={disableForDirector}
        />
      ),
    },
    {
      label: "Claim Handling (1)",
      component: <ClaimHandling1 isEnabled={isEnabled} />,
    },
    {
      label: "Claim Handling (2)",
      component: <ClaimHandling2 isEnabled={isEnabled} />,
    },
    {
      label: "Claim Handling (3)",
      component: <ClaimHandling3 isEnabled={isEnabled} />,
    },
    {
      label: "CCT Instructions Other",
      component: <CctInstructionsOther isEnabled={isEnabled} />,
    },
    {
      label: "CCT Assignment",
      component: <CctAssignment isEnabled={isEnabled} />,
    },
    { label: "SHI Print", component: <ShiPrint isEnabled={isEnabled} /> },
  ];

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await api.get("/affinity_program/", {
          params: { ProgramName: getValues("ProgramName") },
        });
        if (res.status === 200) setAffinityData(res.data[0]);
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
      }
    };

    fetchData();
  }, [searchParams, theme.palette.error.main, getValues]);

  useImperativeHandle(ref, () => ({
    submit: async (actionType) => {
      let isSuccessful = false;

      await handleSubmit(async (data) => {
        const mockEvent = { nativeEvent: { submitter: { name: actionType } } };
        const result = await onSubmit(data, mockEvent);
        isSuccessful = result;
      }, onError)();

      return isSuccessful;
    },
    createNewPolicy: handleNewPolicy,
    createNextMod: handleNextMod,
  }));

  const onSubmit = async (data, e) => {
    const operation = e.nativeEvent.submitter.name;

    const filteredData = Object.fromEntries(
      Object.entries(data).filter(([_, v]) => v !== null && v !== ""),
    );

    //confirmation alert on submission
    if (operation === "submit") {
      const res = await Swal.fire({
        title: "Confirm Submit",
        text: "This will save all the changes & close the policy. Is that what you want to do?",
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

    if (isNextMod) delete filteredData.PK_Number;
    setSubmitOrSave(operation);

    try {
      const res = await api.post("/affinity_policy_types/upsert", filteredData);

      setSubmitOrSave("");

      if (res.status === 200) {
        const res2 = await Swal.fire({
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

        if (res2.isConfirmed || res2.dismiss) {
          if (isStepper) return res.data.pk;
          else if (operation === "submit") {
            reset(defaultValues);
            navigate(from, {
              state: { from: pathname },
              replace: true,
            });
          } else if (operation === "save") {
            const newPath = `/view-policy-types/PK_Number=${res.data.pk}`;
            if (pathname !== newPath) {
              navigate(newPath, {
                replace: true,
              });
            }
          }
          setIsNextMod(false);
          return res.data.pk;
        }
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
      if (operation === "submit" || (isStepper && operation === "save"))
        setIsNextMod(false);
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
    if (searchParams.size === 1 || (isStepper && accountData && !pkProp)) {
      setValue("PolicyStatus", "");
      reset({
        ...defaultValues,
        ProgramName: searchParams.get("ProgramName") || accountData.ProgramName,
      });
      return;
    }

    const fetchData = async (searchByColumn, value) => {
      try {
        setLoading(true);
        const res = await api.get("/affinity_policy_types/", {
          params: { [searchByColumn]: value },
        });

        //replacing all null values with empty string
        const data = Object.fromEntries(
          Object.entries(res.data[0]).map(([k, v]) => [k, v === null ? "" : v]),
        );

        let formattedData = {
          ...data,
          DateCreated: formatDate(data.DateCreated),
          TermDate: formatDate(data.TermDate),
          PolicyType: isNextMod ? "" : data.PolicyType,
        };

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

    if (column_name && !isStepper) {
      const [key, value] = column_name.split("=");
      fetchData(key, value);
    } else if (isStepper && pkProp) fetchData("PK_Number", pkProp);
    else reset(defaultValues);
  }, [
    column_name,
    reset,
    from,
    searchParams,
    accountData,
    isStepper,
    pkProp,
    setValue,
    isNextMod,
    theme.palette.error.main,
  ]);

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

    if (res.isConfirmed) {
      if (isStepper) onReturn();
      else {
        if (isNextMod) setIsNextMod(false);
        else
          navigate(
            `/affinity-view-program/ProgramName=${getValues("ProgramName")}`,
            { state: { from: pathname }, replace: true },
          );
      }
    }
  };

  const handleNewPolicy = () => {
    if (isStepper) onCreatePolicy();
    else {
      navigate(
        `/create-new-policy-type?ProgramName=${getValues("ProgramName")}`,
        { state: { from }, replace: true },
      );
    }
  };

  const handleNextMod = () => {
    setIsNextMod(true);
    if (isStepper && onModStart) onModStart();
  };

  const handleUnderwriterUpdate = async (oldVal, val) => {
    const newVal = UnderwritersData.find((i) => i["UW Last"] === val);
    if (!newVal) return;

    const data = {
      ProgramName: getValues("ProgramName"),
      RecipCat: "Underwriter",
      DistVia: "Email",
      AttnTo: newVal["UW Last"],
      EMailAddress: newVal["UW Email"],
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
        text: "Some error occoured, unable to update data in distribution table",
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

  if (loading) return <Loader size={40} height="600px" />;

  return (
    <FormProvider {...methods}>
      <form
        onSubmit={handleSubmit(onSubmit, onError)}
        noValidate
        style={{ height: "100%" }}
      >
        <Grid
          container
          spacing={1.5}
          direction="row"
          sx={{ height: "100%", alignContent: "flex-start" }}
        >
          <Grid container spacing={1}>
            <Grid
              container
              spacing={1}
              size={12}
              sx={{
                border: "1px solid lightgrey",
                padding: "20px",
                borderRadius: "15px",
                boxShadow: "0 1px 8px rgba(0, 0, 0, 0.25)",
                backgroundColor: "#EBF0F8",
              }}
            >
              <Grid size={{ xs: 12, md: 6 }}>
                <Controller
                  name="ProgramName"
                  control={control}
                  rules={{
                    required: "Program Name is mandatory and cannot be empty",
                  }}
                  render={({ field, fieldState: { error } }) => (
                    <TextField
                      {...field}
                      fullWidth
                      required
                      label="Affinity Program Name"
                      error={!!error}
                      disabled
                    />
                  )}
                />
              </Grid>
              <Grid size={{ xs: 12, md: 3 }}>
                <FormControl fullWidth>
                  <Controller
                    name="PolicyStatus"
                    control={control}
                    disabled={!isEnabled("PolicyStatus")}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
                        options={
                          ddData
                            ?.filter((i) => i.DD_Type === "PolicyStatus")
                            .sort((a, b) => a.DD_SortOrder - b.DD_SortOrder) ||
                          []
                        }
                        loading={ddLoading}
                        getOptionLabel={(option) => option.DD_Value || ""}
                        isOptionEqualToValue={(option, val) =>
                          option.DD_Value === val?.DD_Value
                        }
                        value={
                          value
                            ? ddData.find((opt) => opt.DD_Value === value) || {
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
                            label="Affinity Status"
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
                    name="PolicyType"
                    control={control}
                    rules={{
                      required: "Policy Type is mandatory and cannot be empty",
                    }}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                      fieldState: { error },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
                        disabled={!isEnabled("PolicyType")}
                        options={
                          ddData
                            ?.filter((i) => i.DD_Type === "PolicyType")
                            .sort((a, b) => a.DD_SortOrder - b.DD_SortOrder) ||
                          []
                        }
                        loading={ddLoading}
                        getOptionLabel={(option) => option.DD_Value || ""}
                        isOptionEqualToValue={(option, val) =>
                          option.DD_Value === val?.DD_Value
                        }
                        value={
                          value
                            ? ddData.find((opt) => opt.DD_Value === value) || {
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
                            label="Policy Type"
                            inputRef={ref}
                            disabled={!isEnabled("PolicyType")}
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
                <LocalizationProvider dateAdapter={AdapterDayjs}>
                  <Controller
                    name="DateCreated"
                    control={control}
                    render={({ field: { onChange, value, ...fieldProps } }) => (
                      <DatePicker
                        {...fieldProps}
                        label="Created Date"
                        value={value ? dayjs(value) : null}
                        disabled={!isEnabled("DateCreated")}
                        onChange={(newValue) => {
                          onChange(
                            newValue ? newValue.format("YYYY-MM-DD") : null,
                          );
                        }}
                      />
                    )}
                  />
                </LocalizationProvider>
              </Grid>
              <Grid size={{ xs: 12, md: 3 }}>
                <FormControl fullWidth>
                  <Controller
                    name="PolicyBusinessType"
                    control={control}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
                        disabled={!isEnabled("PolicyBusinessType")}
                        options={
                          ddData
                            ?.filter((i) => i.DD_Type === "GenPolicyStatus")
                            .sort((a, b) => a.DD_SortOrder - b.DD_SortOrder) ||
                          []
                        }
                        loading={ddLoading}
                        getOptionLabel={(option) => option.DD_Value || ""}
                        isOptionEqualToValue={(option, val) =>
                          option.DD_Value === val?.DD_Value
                        }
                        value={
                          value
                            ? ddData.find((opt) => opt.DD_Value === value) || {
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
                            label="Business Status"
                            inputRef={ref}
                            disabled={!isEnabled("PolicyBusinessType")}
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
                    name="UnderwriterName"
                    control={control}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
                        disabled={!isEnabled("UnderwriterName")}
                        options={UnderwritersData || []}
                        loading={UnderwritersLoading}
                        getOptionLabel={(option) => option["UW Last"] || ""}
                        isOptionEqualToValue={(option, val) =>
                          option["UW Last"] === val["UW Last"]
                        }
                        value={
                          value
                            ? UnderwritersData.find(
                                (opt) => opt["UW Last"] === value,
                              ) || { "UW Last": value }
                            : null
                        }
                        onChange={(_, newValue) => {
                          const selectedvalue = newValue
                            ? newValue["UW Last"]
                            : "";
                          onChange(selectedvalue);
                          handleUnderwriterUpdate(value, selectedvalue);
                        }}
                        renderInput={(params) => (
                          <TextField
                            {...params}
                            label="Underwriter"
                            inputRef={ref}
                            disabled={!isEnabled("UnderwriterName")}
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
                    name="UWMgr"
                    control={control}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
                        disabled={!isEnabled("UWMgr")}
                        options={UnderwritersData || []}
                        loading={UnderwritersLoading}
                        getOptionLabel={(option) => option["UW Last"] || ""}
                        isOptionEqualToValue={(option, val) =>
                          option["UW Last"] === val["UW Last"]
                        }
                        value={
                          value
                            ? UnderwritersData.find(
                                (opt) => opt["UW Last"] === value,
                              ) || { "UW Last": value }
                            : null
                        }
                        onChange={(_, newValue) => {
                          onChange(newValue ? newValue["UW Last"] : "");
                        }}
                        renderInput={(params) => (
                          <TextField
                            {...params}
                            label="Underwriting Manager"
                            inputRef={ref}
                            disabled={!isEnabled("UWMgr")}
                          />
                        )}
                      />
                    )}
                  />
                </FormControl>
              </Grid>

              <Grid size={{ xs: 12, md: 4 }}>
                <FormControl fullWidth>
                  <InputLabel>Is Location Coded?</InputLabel>
                  <Controller
                    name="LOCCoded"
                    control={control}
                    disabled={!isEnabled("LOCCoded")}
                    render={({ field }) => (
                      <Select {...field} label="Is Location Coded?">
                        <MenuItem value="Yes">Yes</MenuItem>
                        <MenuItem value="No">No</MenuItem>
                      </Select>
                    )}
                  />
                </FormControl>
              </Grid>
              <Grid size={{ xs: 12, md: 4 }}>
                <FormControl fullWidth>
                  <Controller
                    name="TermReason"
                    control={control}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
                        disabled={!isEnabled("TermReason")}
                        options={TermReasonData || []}
                        loading={TermReasonLoading}
                        getOptionLabel={(option) => option.DD_Value || ""}
                        isOptionEqualToValue={(option, val) =>
                          option.DD_Value === val?.DD_Value
                        }
                        value={
                          value
                            ? TermReasonData.find(
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
                            label="Termination Reason"
                            inputRef={ref}
                            disabled={!isEnabled("TermReason")}
                          />
                        )}
                      />
                    )}
                  />
                </FormControl>
              </Grid>
              <Grid size={{ xs: 12, md: 4 }}>
                <LocalizationProvider dateAdapter={AdapterDayjs}>
                  <Controller
                    name="TermDate"
                    control={control}
                    render={({ field: { onChange, value, ...fieldProps } }) => (
                      <DatePicker
                        {...fieldProps}
                        label="Termination Date"
                        value={value ? dayjs(value) : null}
                        disabled={!isEnabled("TermDate")}
                        onChange={(newValue) => {
                          onChange(
                            newValue ? newValue.format("YYYY-MM-DD") : null,
                          );
                        }}
                      />
                    )}
                  />
                </LocalizationProvider>
              </Grid>
            </Grid>

            <Box
              sx={{
                width: "100%",
                border: "1px solid lightgrey",
                padding: "20px 20px 10px 20px",
                borderRadius: "15px",
                boxShadow: "0 1px 8px rgba(0, 0, 0, 0.25)",
              }}
            >
              <Tabs
                value={tabIndex}
                onChange={handleTabChange}
                variant="scrollable"
                scrollButtons="auto"
                aria-label="account tabs"
                sx={{ mb: 1.5, ".MuiTabs-indicator": { display: "none" } }}
              >
                {tabList.map((tab) => (
                  <Tab
                    key={tab.label}
                    label={tab.label}
                    sx={{
                      borderRadius: "50px",
                      backgroundColor: "#EBF0F8",
                      mr: 1.5,
                      "&.Mui-selected": {
                        color: "white",
                        background:
                          "linear-gradient(90deg, hsla(303, 77%, 22%, 1) 0%, hsla(321, 55%, 50%, 1) 100%)",
                      },
                    }}
                  />
                ))}
              </Tabs>

              {tabList.map((tab, i) => (
                <TabPanel key={i} value={tabIndex} index={i}>
                  {tab.component}
                </TabPanel>
              ))}
            </Box>

            <Grid
              sx={{
                width: "100%",
                display: "flex",
                justifyContent: "space-between",
                marginTop: "auto",
              }}
            >
              {!isStepper && (
                <Grid
                  sx={{
                    display: "flex",
                    gap: 2,
                  }}
                >
                  {((!isStepper &&
                    pathname !== "/create-new-policy-type" &&
                    !isNextMod) ||
                    (isStepper && pkProp && !isNextMod)) && (
                    <>
                      <Button
                        variant="outlined"
                        color="primary"
                        onClick={handleNewPolicy}
                        disabled={disableForDirector}
                      >
                        Create New Policy Type
                      </Button>
                      <Button
                        variant="outlined"
                        color="primary"
                        onClick={handleNextMod}
                        disabled={disableForDirector}
                      >
                        Create Next Policy Type
                      </Button>
                    </>
                  )}
                </Grid>
              )}

              {!isStepper && (
                <Grid
                  sx={{
                    display: "flex",
                    gap: 2,
                  }}
                >
                  <Button
                    type="button"
                    variant="outlined"
                    onClick={handleBack}
                    disabled={submitOrSave !== ""}
                  >
                    Back
                  </Button>
                  <Button
                    type="submit"
                    variant="outlined"
                    color="primary"
                    name="save"
                    disabled={submitOrSave !== "" || disableForDirector}
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
                    disabled={submitOrSave !== "" || disableForDirector}
                  >
                    {submitOrSave === "submit" ? (
                      <CircularProgress size={20} color="inherit" />
                    ) : (
                      "Submit"
                    )}
                  </Button>
                </Grid>
              )}
            </Grid>
          </Grid>
        </Grid>
      </form>
    </FormProvider>
  );
});

export default CreateNewPolicyTypes;
