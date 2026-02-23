import {
  Select,
  MenuItem,
  InputLabel,
  FormControl,
  Grid,
  FormControlLabel,
  RadioGroup,
  Radio,
  FormLabel,
  Box,
  Table,
  TableBody,
  TableRow,
  TableCell,
  TableHead,
  Paper,
  useTheme,
  createFilterOptions,
  Autocomplete,
  TextField,
} from "@mui/material";
import { Fragment, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import api from "../../../../../api/api";
import Loader from "../../../../ui/Loader";
import Swal from "sweetalert2";

const CustomPaper = (props) => {
  return (
    <Paper
      {...props}
      sx={{
        marginTop: "5px",
        borderRadius: "15px",
        boxShadow: "0 1px 8px rgba(0, 0, 0, 0.25)",
        minWidth: "700px",
      }}
    />
  );
};

export default function ViewAffinityProgram() {
  const [options, setOptions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchBy, setSearchBy] = useState("");
  const navigate = useNavigate();
  const [highlightedOption, setHighlightedOption] = useState(null);
  const theme = useTheme();

  const openAccount = (id) => {
    navigate(`/affinity-view-program/ProgramName=${id}`, {
      state: { from: "/affinity-view-program" },
      replace: true,
    });
  };

  const columns = options.length > 0 && Object.keys(options[0]);
  const dynamicSearchBy =
    searchBy === "AccountName"
      ? "Customer Name"
      : searchBy === "PolicyNameInsured"
      ? "Name Insured on Policy"
      : searchBy.replace(/([a-z])([A-Z])/g, "$1 $2").replace("Num", "Number");

  const filterOptions = createFilterOptions({
    matchFrom: "any",
    stringify: (option) => option[dynamicSearchBy] ?? "",
    limit: 100,
  });

  useEffect(() => {
    const loadOptions = async () => {
      setLoading(true);
      try {
        const res = await api.get(`/search_affinity_program/`, {
          params: {
            search_by: searchBy,
          },
        });
        if (res.status === 200) setOptions(res.data);
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

    if (searchBy) loadOptions();
  }, [searchBy, theme.palette.error.main]);

  return (
    <Grid container spacing={2} mt={2}>
      <Grid size={2.5} />
      <Grid size={3}>
        <FormControl
          component="fieldset"
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "center",
            gap: 2,
            marginLeft: 2,
          }}
        >
          <FormLabel id="autoDelivery">Is this Affinity Business?</FormLabel>
          <RadioGroup
            row
            defaultValue="Yes"
            onChange={(e) => {
              if (e.target.value === "No")
                navigate("/sac-view-account", { replace: true });
            }}
          >
            <FormControlLabel value="Yes" control={<Radio />} label="Yes" />
            <FormControlLabel value="No" control={<Radio />} label="No" />
          </RadioGroup>
        </FormControl>
      </Grid>
      <Grid size={4}>
        <FormControl fullWidth>
          <InputLabel>Search By</InputLabel>
          <Select
            label="Search By"
            value={searchBy}
            onChange={(e) => setSearchBy(e.target.value)}
          >
            <MenuItem value="ProgramName">Program Name</MenuItem>
            <MenuItem value="ProducerCode">Producer Code</MenuItem>
          </Select>
        </FormControl>
      </Grid>
      <Grid size={2.5} />

      <Grid size={2.5} />
      <Grid size={7}>
        <Autocomplete
          filterOptions={filterOptions}
          options={options}
          loading={loading}
          slots={{ paper: CustomPaper }}
          onHighlightChange={(_, option) => setHighlightedOption(option)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && highlightedOption) {
              openAccount(highlightedOption["Program Name"]);
              e.defaultMuiPrevented = true;
            }
          }}
          renderInput={(params) => (
            <TextField
              {...params}
              label="Use this list to select your account"
              variant="outlined"
              slotProps={{
                input: {
                  ...params.InputProps,
                  endAdornment: (
                    <>
                      {loading ? <Loader size={16} height="30px" /> : null}
                      {params.InputProps.endAdornment}
                    </>
                  ),
                },
              }}
            />
          )}
          getOptionLabel={(option) => option[dynamicSearchBy] ?? ""}
          groupBy={() => "header"}
          renderGroup={(params) => (
            <Fragment key={params.key}>
              <Table size="small" sx={{ tableLayout: "fixed" }}>
                <TableHead>
                  <TableRow>
                    {columns.map((col) => (
                      <TableCell
                        key={col}
                        sx={{ fontWeight: "bold", whiteSpace: "nowrap" }}
                      >
                        {col}
                      </TableCell>
                    ))}
                  </TableRow>
                </TableHead>
              </Table>
              {params.children}
            </Fragment>
          )}
          renderOption={(props, option) => {
            const { key, ...rest } = props;
            return (
              <Box
                key={props["data-option-index"]}
                {...rest}
                sx={{ borderRadius: "25px" }}
                onClick={() => openAccount(option["Program Name"])}
              >
                <Table size="small" sx={{ tableLayout: "fixed" }}>
                  <TableBody>
                    <TableRow>
                      {columns.map((col) => (
                        <TableCell key={col}>
                          {col === "Service Level"
                            ? option[col]?.slice(2)
                            : option[col]}{" "}
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableBody>
                </Table>
              </Box>
            );
          }}
        />{" "}
      </Grid>
    </Grid>
  );
}
