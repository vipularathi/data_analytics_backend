import {
  Card,
  CardContent,
  CardHeader,
  useTheme,
} from "@mui/material";
import { observer } from "mobx-react-lite";

const ChartCard = observer(({ title, children }) => {
  const theme = useTheme();
  return (
    <div className="p-8">
      <Card className=" w-full shadow-2">
        <CardHeader
          title={title}
          titleTypographyProps={{
            color: theme.palette.chart.headingColor,
            fontSize: "1.4rem",
          }}
        />
        <CardContent>{children}</CardContent>
      </Card>
    </div>
  );
});

export default ChartCard;
