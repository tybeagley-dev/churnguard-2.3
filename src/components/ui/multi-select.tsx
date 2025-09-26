import { useState, useRef, useEffect } from "react";
import { Check, ChevronDown, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";

interface MultiSelectProps {
  options: string[];
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
  maxDisplay?: number;
}

export function MultiSelect({ 
  options, 
  value, 
  onChange, 
  placeholder = "Select items...",
  maxDisplay = 2 
}: MultiSelectProps) {
  const [open, setOpen] = useState(false);

  const handleSelectAll = () => {
    if (value.length === options.length) {
      onChange([]);
    } else {
      onChange(options);
    }
  };

  const handleToggleOption = (option: string) => {
    if (value.includes(option)) {
      onChange(value.filter(v => v !== option));
    } else {
      onChange([...value, option]);
    }
  };

  const getDisplayText = () => {
    if (value.length === 0) {
      return placeholder;
    } else if (value.length <= maxDisplay) {
      return value.join(", ");
    } else if (value.length === options.length) {
      return "All Selected";
    } else {
      return `${value.length} selected`;
    }
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-48 justify-between text-left font-normal"
        >
          <span className="truncate">{getDisplayText()}</span>
          <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-48 p-0" align="start">
        <div className="border-b p-2">
          <div className="flex items-center space-x-2">
            <Checkbox
              id="select-all"
              checked={value.length === options.length}
              onCheckedChange={handleSelectAll}
            />
            <label
              htmlFor="select-all"
              className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
            >
              Select All
            </label>
          </div>
        </div>
        <div className="max-h-60 overflow-auto">
          {options.map((option) => (
            <div
              key={option}
              className="flex items-center space-x-2 p-2 hover:bg-gray-50 cursor-pointer"
              onClick={() => handleToggleOption(option)}
            >
              <Checkbox
                id={option}
                checked={value.includes(option)}
                onCheckedChange={() => handleToggleOption(option)}
              />
              <label
                htmlFor={option}
                className="text-sm leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer"
              >
                {option}
              </label>
            </div>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  );
}