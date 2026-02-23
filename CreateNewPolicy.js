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
  useTheme,
  CircularProgress,
  Autocomplete,
} from "@mui/material";
import { forwardRef, useEffect, useImperativeHandle, useState } from "react";
import { useForm, Controller, FormProvider } from "react-hook-form";
import {
  useLocation,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import Policy from "./Policy";
import InsuredContacts from "./InsuredContacts";
import ClaimHandling1 from "./ClaimHandling1";
import ClaimHandling2 from "./ClaimHandling2";
import ClaimHandling3 from "./ClaimHandling3";
import CctInstructionsOther from "./CctInstructionsOther";
import Agent from "./Agent";
import CctInstructionsPolicy from "./CctInstructionsPolicy";
import CctAssignment from "./CctAssignment";
import TabPanel from "../../../../ui/TabPanel";
import api from "../../../../../api/api";
import Loader from "../../../../ui/Loader";
import Deductible from "./Deductible";
import Swal from "sweetalert2";
import ShiPrint from "../../../ShiPrint";
import { policyFieldPermissions } from "../../../../../field-permissions";
import { useSelector } from "react-redux";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import dayjs from "dayjs";
import { formatDate } from "../../../../../util";
import useDropdownData from "../../../../../hooks/useDropdownData";

const defaultValues = {
  PolPref: "",
  PolicyNum: "",
  PolMod: "",
  AccountName: "",
  CustomerNum: "",
  AcctOnPolicyName: "",
  PolicyStatus: "",
  LocList: "",
  LocCoded: "",
  InceptDate: "",
  LocCompDate: "",
  ExpDate: "",
  PolicyType: "",
  PolicyBusinessType: "",
  PolicyNotes: "",
  //Policy
  DateCreated: new Date().toISOString().split("T")[0],
  CreatedBy: "",
  UnderwriterName: "",
  UWMgr: "",
  DNRDate: "",
  DNRStatus: "",
  RenewDiaryDT: "",
  PremiumAmt: "",
  //Agent
  AgentName: "",
  AgentCode: "",
  AgentSeg: "",
  AgentContact1: "",
  AgentTel1: "",
  AgentCell1: "",
  AgentFax1: "",
  AgentEmail1: "",
  AgentContact2: "",
  AgentTel2: "",
  AgentCell2: "",
  AgentFax2: "",
  AgentEmail2: "",
  //Insured contact
  InsuredContact1: "",
  InsuredTitle1: "",
  InsuredPhone1: "",
  InsuredCell1: "",
  InsuredEMail1: "",
  InsuredContact2: "",
  InsuredTitle2: "",
  InsuredPhone2: "",
  InsuredCell2: "",
  InsuredEMail2: "",
  InsuredNotes: "",
  AdjusterContact1: "",
  AdjusterTitle1: "",
  AdjusterPhone1: "",
  AdjusterCell1: "",
  AdjusterEMail1: "",
  AdjusterContact2: "",
  AdjusterTitle2: "",
  AdjusterPhone2: "",
  AdjusterCell2: "",
  AdjusterEMail2: "",
  //deductiable
  LargeDeductYN: "No",
  BillExpYN: "No",
  BillName: "",
  AggMet: "No",
  AggAmt: "0",
  LCFrate: "0",
  LCYN: "No",
  LCAmt: "0",
  LCBank: "",
  PerClaimAggAmt: "0.00",
  FeatType: "",
  SentParagon: "",
  DeductNotesOne: "",
  DeductNotesTwo: "",
  //claim handling1
  ContactInstruct: "",
  CoverageInstruct: "",
  //claim handling2
  PrefCounselYN: "No",
  FirmName: "",
  FirmAddress: "",
  FirmCityStateZip: "",
  FirmWebsite: "",
  CounselName: "",
  CounselPhone: "",
  CounselEmail: "",
  HourlyRatesPartner: "",
  HourlyRatesAssociate: "",
  HourlyRatesParalegal: "",
  LitigationInstruct: "",
  //claim handling3
  RecoveryInstruct: "",
  MiscCovInstruct: "",
  //cct instructions policy
  CCTBusLine: "",
  UnManPol: "",
  AutoPolYN: "",
  RentedHired: "",
  AcctProdClaims: "No",
  AcctValetCov: "No",
  WCCClassCodeYN: "",
  WCClassNotes: "",
  NoLocYN: "",
  NoLocNotes: "",
  //cct instructions other
  HCSSupport: "",
  AddLDocs: "No",
  AcctLocID: "",
  PMSUnit: "",
  AcctLocNotes: "",
  PMSUnitNotes: "",
  CCTOtherNotes: "",
  //cct assignment
  SpecHand: "Auto Assign",
  CCTAssgInstruct: "",
};

const CreateNewPolicy = forwardRef((props, ref) => {
  const methods = useForm({ defaultValues });
  const { handleSubmit, control, reset, setValue, getValues } = methods;
  const [tabIndex, setTabIndex] = useState(0);
  const [loading, setLoading] = useState(false);
  const { column_name } = useParams();
  const navigate = useNavigate();
  const [submitOrSave, setSubmitOrSave] = useState("");
  const { pathname, state } = useLocation();
  const from = state?.from || "/pending-items";
  const theme = useTheme();
  const [showCompDt, setShowCompDt] = useState(false);
  const [showCancelDt, setShowCancelDt] = useState(false);
  const [searchParams] = useSearchParams();
  const user = useSelector((state) => state.auth.user);
  const allowedFields = policyFieldPermissions[user.role];
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
  const [sacData, setSacData] = useState({});
  const disableForDirector =
    user.role === "Director" &&
    sacData?.Stage === "Admin" &&
    sacData?.IsSubmitted === 1;

  const isEnabled = (fieldName) => {
    if (allowedFields === "ALL") return true;
    return allowedFields?.includes(fieldName);
  };

  const tabList = [
    {
      label: "Policy",
      component: (
        <Policy isEnabled={isEnabled} disableForDirector={disableForDirector} />
      ),
    },
    {
      label: "Agent",
      component: (
        <Agent isEnabled={isEnabled} disableForDirector={disableForDirector} />
      ),
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
      label: "Deductible",
      component: (
        <Deductible
          isEnabled={isEnabled}
          isNextMod={isNextMod}
          pkProp={pkProp}
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
      label: "CCT Instructions Policy",
      component: <CctInstructionsPolicy isEnabled={isEnabled} />,
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
        const res = await api.get("/sac_account/", {
          params: { CustomerNum: getValues("CustomerNum") },
        });
        if (res.status === 200) setSacData(res.data[0]);
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

    if (operation === "submit") {
      //confirmation alert on submission
      const confirmRes = await Swal.fire({
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
      if (confirmRes.dismiss === Swal.DismissReason.cancel) return;
    }

    const updatedData = {
      ...filteredData,
      PremiumAmt: filteredData.PremiumAmt
        ? String(filteredData.PremiumAmt)
        : null,
    };

    setSubmitOrSave(operation);

    try {
      //Check if same policy num + mod already exists or not, in case of inserts
      if (
        pathname === "/create-new-policy" ||
        isNextMod ||
        (isStepper && !pkProp)
      ) {
        const res1 = await api.get("/sac_policies/", {
          params: {
            PolicyNum: updatedData.PolicyNum,
            PolMod: updatedData.PolMod,
          },
        });

        if (res1.status === 200 && res1.data?.length > 0) {
          Swal.fire({
            title: "Data Validation Error",
            text: "This Policy Number and Mod combination already exists, duplicate records are not permitted",
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

      const res2 = await api.post("/sac_policies/upsert", updatedData);

      setSubmitOrSave("");

      if (res2.status === 200) {
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
          if (isStepper) return res2.data.pk;
          else if (operation === "submit") {
            reset(defaultValues);
            navigate(from, {
              state: { from: pathname },
              replace: true,
            });
          } else if (operation === "save") {
            const newPath = `/view-policy/PK_Number=${res2.data.pk}`;
            if (pathname !== newPath) {
              navigate(newPath, {
                replace: true,
              });
            }
          }
          setIsNextMod(false);
          return res2.data.pk;
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
      return false;
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
    if (searchParams.size === 2 || (isStepper && accountData && !pkProp)) {
      setValue("PolicyStatus", "");
      reset({
        ...defaultValues,
        CustomerNum: searchParams.get("CustomerNum") || accountData.CustomerNum,
        AccountName:
          searchParams.get("CustomerName") || accountData.CustomerName,
      });
      return;
    }

    const fetchData = async (searchByColumn, value) => {
      try {
        setLoading(true);
        const res = await api.get("/sac_policies/", {
          params: { [searchByColumn]: value },
        });

        //replacing all null values with empty string
        const data = Object.fromEntries(
          Object.entries(res.data[0]).map(([k, v]) => [k, v === null ? "" : v]),
        );

        let formattedData = {
          ...data,
          InceptDate:
            isNextMod && data.InceptDate
              ? dayjs(formatDate(data.InceptDate))
                  .add(1, "year")
                  .format("YYYY-MM-DD")
              : formatDate(data.InceptDate),
          LocCompDate: formatDate(data.LocCompDate),
          ExpDate:
            isNextMod && data.ExpDate
              ? dayjs(formatDate(data.ExpDate))
                  .add(1, "year")
                  .format("YYYY-MM-DD")
              : formatDate(data.ExpDate),
          DateCreated: isNextMod
            ? new Date().toISOString().split("T")[0]
            : formatDate(data.DateCreated),
          DNRDate: formatDate(data.DNRDate),
          RenewDiaryDT: formatDate(data.RenewDiaryDT),
          PolMod: isNextMod
            ? String(parseInt(data.PolMod) + 1).padStart(2, "0")
            : data.PolMod,
          PolicyStatus: isNextMod ? "Pending Renewal" : data.PolicyStatus,
          PolicyBusinessType: isNextMod
            ? "Renewal Business"
            : data.PolicyBusinessType,
        };

        if (user.role === "Admin") {
          if (
            formattedData.PolicyStatus === "Canceled" ||
            formattedData.PolicyStatus === "Canceled - Mod Bump" ||
            formattedData.PolicyStatus === "Canceled - Rewrite" ||
            formattedData.PolicyStatus === "Non-Renewal" ||
            formattedData.LocList === "Completed"
          ) {
            setShowCompDt(true);
          } else setShowCompDt(false);

          if (
            formattedData.PolicyStatus === "Canceled" ||
            formattedData.PolicyStatus === "Canceled - Mod Bump" ||
            formattedData.PolicyStatus === "Canceled - Rewrite" ||
            formattedData.PolicyStatus === "Cancellation Pending" ||
            formattedData.PolicyStatus === "Non-Renewal"
          )
            setShowCancelDt(true);
          else setShowCancelDt(false);
        }

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
    isStepper,
    accountData,
    pkProp,
    isNextMod,
    setValue,
    user.role,
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
            `/sac-view-account/CustomerNum=${getValues("CustomerNum")}`,
            { state: { from: pathname }, replace: true },
          );
      }
    }
  };

  const handlePolicyStatus = (val) => {
    setValue("PolicyStatus", val);

    if (val === "Pending Renewal") {
      setValue("PolicyBusinessType", "Renewal Business");
      setValue("UnManPol", "1");
    } else setValue("UnManPol", "2");

    if (
      val === "Canceled" ||
      val === "Canceled - Mod Bump" ||
      val === "Canceled - Rewrite" ||
      val === "Non-Renewal"
    ) {
      setShowCompDt(true);
      setValue("LocCompDate", new Date().toISOString().split("T")[0]);
    }

    if (
      val === "Active" ||
      val === "Cancellation Pending" ||
      val === "Expired Mod" ||
      val === "Fronted Policy - Not Hanover Paper" ||
      val === "New Business - Not in PMS Yet" ||
      val === "Pending Renewal" ||
      val === "Runoff"
    ) {
      setShowCompDt(false);
      setValue("LocCompDate", "");
    }

    if (
      val === "Canceled" ||
      val === "Canceled - Mod Bump" ||
      val === "Canceled - Rewrite" ||
      val === "Cancellation Pending" ||
      val === "Non-Renewal"
    ) {
      setShowCancelDt(true);
    } else setShowCancelDt(false);
  };

  const handleLocationList = (e) => {
    setValue("LocList", e.target.value);

    if (e.target.value === "Completed") {
      setShowCompDt(true);
      setValue("LocCompDate", new Date().toISOString().split("T")[0]);
    } else {
      setShowCompDt(false);
      setValue("LocCompDate", "");
    }
  };

  const handleNewPolicy = () => {
    if (isStepper) onCreatePolicy();
    else {
      navigate(
        `/create-new-policy?CustomerNum=${getValues(
          "CustomerNum",
        )}&CustomerName=${getValues("AccountName")}`,
        { state: { from }, replace: true },
      );
    }
  };

  const handleNextMod = () => {
    setIsNextMod(true);
    if (isStepper && onModStart) onModStart();
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
              <Grid size={{ xs: 12, md: 1 }}>
                <Controller
                  name="PolPref"
                  control={control}
                  disabled={!isEnabled("PolPref")}
                  rules={{
                    required: "Policy Prefix is mandatory and cannot be empty",
                    validate: (value) => {
                      if (!value || value.length === 3) return true;
                      return "Policy Prefix must be 3 characters";
                    },
                  }}
                  render={({
                    field: { onChange, value, ...field },
                    fieldState: { error },
                  }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Prefix"
                      required
                      error={!!error}
                      slotProps={{
                        htmlInput: {
                          maxLength: 3,
                        },
                      }}
                      value={value}
                      onChange={(e) => onChange(e.target.value.toUpperCase())}
                    />
                  )}
                />
              </Grid>
              <Grid size={{ xs: 12, md: 3 }}>
                <Controller
                  name="PolicyNum"
                  control={control}
                  rules={{
                    required: "Policy Number is mandatory and cannot be empty",
                    validate: (value) => {
                      if (value.length === 7) return true;
                      return "Policy Number must be 7 characters";
                    },
                  }}
                  render={({ field, fieldState: { error } }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Policy #"
                      required
                      disabled={!isEnabled("PolicyNum")}
                      error={!!error}
                      slotProps={{
                        htmlInput: {
                          maxLength: 7,
                        },
                      }}
                      onChange={(e) => {
                        field.onChange(e.target.value.toUpperCase());
                      }}
                    />
                  )}
                />
              </Grid>
              <Grid size={{ xs: 12, md: 1 }}>
                <Controller
                  name="PolMod"
                  control={control}
                  rules={{
                    required: "Policy Mod is mandatory and cannot be empty",
                  }}
                  render={({ field, fieldState: { error } }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Mod"
                      required
                      disabled={!isEnabled("PolMod")}
                      onChange={(e) => {
                        const raw = e.target.value.replace(/[^0-9]/g, "");

                        if (raw.length <= 2) field.onChange(raw);
                        else if (raw.length === 3 && raw.startsWith("0"))
                          field.onChange(raw.slice(1));
                        else field.onChange(raw.slice(0, 2));
                      }}
                      onBlur={(e) => {
                        field.onBlur(e);
                        if (field.value && field.value.length === 1) {
                          field.onChange("0" + field.value);
                        }
                      }}
                      error={!!error}
                    />
                  )}
                />
              </Grid>
              <Grid size={{ xs: 12, md: 3.5 }}>
                <Controller
                  name="AccountName"
                  control={control}
                  rules={{
                    required: "Customer Name is mandatory and cannot be empty",
                  }}
                  render={({ field, fieldState: { error } }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Customer Name"
                      required
                      disabled={true}
                      error={!!error}
                    />
                  )}
                />
              </Grid>
              <Grid size={{ xs: 12, md: 3.5 }}>
                <Controller
                  name="CustomerNum"
                  control={control}
                  rules={{
                    required: "Customer Name is mandatory and cannot be empty",
                  }}
                  render={({ field, fieldState: { error } }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Customer #"
                      required
                      disabled={true}
                      error={!!error}
                      onChange={(e) => {
                        // Replace anything that is NOT a number (0-9) with an empty string
                        field.onChange(e.target.value.replace(/[^0-9]/g, ""));
                      }}
                    />
                  )}
                />
              </Grid>

              <Grid size={{ xs: 12, md: 3 }}>
                <Controller
                  name="AcctOnPolicyName"
                  control={control}
                  disabled={!isEnabled("AcctOnPolicyName")}
                  render={({ field }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Policy Name Insured"
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
                          const val = newValue ? newValue.DD_Value : "";
                          onChange(val);
                          handlePolicyStatus(val);
                        }}
                        renderInput={(params) => (
                          <TextField
                            {...params}
                            label="Policy Status"
                            inputRef={ref}
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
                    name="CanceledDate"
                    control={control}
                    render={({ field: { onChange, value, ...fieldProps } }) => (
                      <DatePicker
                        {...fieldProps}
                        label="Canceled Date"
                        value={value ? dayjs(value) : null}
                        disabled={
                          !isEnabled("CanceledDate") ||
                          (isEnabled("CanceledDate") && !showCancelDt)
                        }
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
                  <InputLabel required>Is Location Coded?</InputLabel>
                  <Controller
                    name="LocCoded"
                    control={control}
                    disabled={!isEnabled("LocCoded")}
                    rules={{
                      required:
                        "Location Coded is mandatory and cannot be empty",
                    }}
                    render={({ field, fieldState: { error } }) => (
                      <Select
                        {...field}
                        label="Is Location Coded?"
                        required
                        error={!!error}
                      >
                        <MenuItem value="Yes">Yes</MenuItem>
                        <MenuItem value="No">No</MenuItem>
                      </Select>
                    )}
                  />
                </FormControl>
              </Grid>

              <Grid size={{ xs: 12, md: 4 }}>
                <LocalizationProvider dateAdapter={AdapterDayjs}>
                  <Controller
                    name="InceptDate"
                    control={control}
                    render={({ field: { onChange, value, ...fieldProps } }) => (
                      <DatePicker
                        {...fieldProps}
                        label="Inception Date"
                        value={value ? dayjs(value) : null}
                        disabled={!isEnabled("InceptDate")}
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
              <Grid size={{ xs: 12, md: 4 }}>
                <LocalizationProvider dateAdapter={AdapterDayjs}>
                  <Controller
                    name="ExpDate"
                    control={control}
                    render={({ field: { onChange, value, ...fieldProps } }) => (
                      <DatePicker
                        {...fieldProps}
                        label="Expiration Date"
                        value={value ? dayjs(value) : null}
                        disabled={!isEnabled("ExpDate")}
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
              <Grid size={{ xs: 12, md: 4 }}>
                <FormControl fullWidth>
                  <InputLabel>Location List</InputLabel>
                  <Controller
                    name="LocList"
                    control={control}
                    disabled={!isEnabled("LocList")}
                    render={({ field }) => (
                      <Select
                        {...field}
                        label="Location List"
                        onChange={handleLocationList}
                      >
                        <MenuItem value="N/A">N/A</MenuItem>
                        <MenuItem value="Completed">Completed</MenuItem>
                        <MenuItem value="In Progress - Set Activity for Michelle">
                          In Progress - Set Activity for Michelle
                        </MenuItem>
                        <MenuItem value="Pending UW - Set Activity for Michelle">
                          Pending UW - Set Activity for Michelle
                        </MenuItem>
                      </Select>
                    )}
                  />
                </FormControl>
              </Grid>

              <Grid size={{ xs: 12, md: 4 }}>
                <FormControl fullWidth>
                  <Controller
                    name="PolicyType"
                    control={control}
                    disabled={!isEnabled("PolicyType")}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
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
                          />
                        )}
                      />
                    )}
                  />
                </FormControl>
              </Grid>
              <Grid size={{ xs: 12, md: 4 }}>
                <FormControl fullWidth>
                  <Controller
                    name="PolicyBusinessType"
                    control={control}
                    disabled={!isEnabled("PolicyBusinessType")}
                    render={({
                      field: { onChange, value, ref, ...fieldProps },
                    }) => (
                      <Autocomplete
                        {...fieldProps}
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
                            label="Policy Business Status"
                            inputRef={ref}
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
                    name="LocCompDate"
                    control={control}
                    render={({ field: { onChange, value, ...fieldProps } }) => (
                      <DatePicker
                        {...fieldProps}
                        label="Location Completion Date"
                        value={value ? dayjs(value) : null}
                        disabled={
                          !isEnabled("LocCompDate") ||
                          (isEnabled("LocCompDate") && !showCompDt)
                        }
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

              <Grid size={{ xs: 12, md: 12 }}>
                <Controller
                  name="PolicyNotes"
                  control={control}
                  disabled={!isEnabled("PolicyNotes")}
                  render={({ field }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Notes"
                      multiline
                      rows={3}
                    />
                  )}
                />
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
          </Grid>

          <Grid
            sx={{
              width: "100%",
              display: "flex",
              justifyContent: "space-between",
            }}
          >
            {!isStepper && (
              <Grid container spacing={2}>
                {((!isStepper &&
                  pathname !== "/create-new-policy" &&
                  !isNextMod) ||
                  (isStepper && pkProp && !isNextMod)) && (
                  <>
                    <Button
                      variant="outlined"
                      color="primary"
                      onClick={handleNewPolicy}
                      disabled={disableForDirector}
                    >
                      Create a Brand New Policy
                    </Button>
                    <Button
                      variant="outlined"
                      color="primary"
                      onClick={handleNextMod}
                      disabled={disableForDirector}
                    >
                      Create Next Mod on Same Policy
                    </Button>
                  </>
                )}
              </Grid>
            )}

            {!isStepper && (
              <Grid
                sx={{
                  height: "40px",
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
      </form>
    </FormProvider>
  );
});

export default CreateNewPolicy;
